from __future__ import division, absolute_import, print_function

from cStringIO import StringIO
from collections import Counter

import requests
from psycopg2.extensions import cursor
from gevent.pool import Pool

from idb.helpers.memoize import memoized
from idb.postgres_backend import apidbpool
from idb.postgres_backend.db import MediaObject, PostgresDB
from idb.helpers.storage import IDigBioStorage
from idb.helpers.media_validation import UnknownMediaTypeError, get_validator
from idb.helpers.logging import idblogger
from . import check_ignore_media


logger = idblogger.getChild('mediaing')

POOL_SIZE = 5
LAST_CHECK_INTERVAL = '1 month'
REQ_CONNECT_TIMEOUT = 15  # seconds

USER_AGENT = 'iDigBio Media Ingestor (idigbio@acis.ufl.edu https://www.idigbio.org/wiki/index.php/Media_Ingestor)'

@memoized()
def rsess():
    s = requests.Session()
    adapter = requests.adapters.HTTPAdapter(pool_connections=POOL_SIZE*2, pool_maxsize=POOL_SIZE*2)
    s.mount('http://', adapter)
    s.mount('https://', adapter)
    s.headers['User-Agent'] = USER_AGENT
    return s


def write_bad(url, content):
    url_path = "bad_media/" + url.replace("/", "^^")
    with open(url_path, "wb") as outf:
        outf.write(content)


class GetMediaError(Exception):
    status = None
    url = None
    inner = None

    def __init__(self, url, status, inner=None):
        self.url = url
        self.status = status
        self.inner = inner
        self.message = "Failed GET %s %r" % (self.url, self.status)

    def __str__(self):
        return self.message


class ReqFailure(GetMediaError):
    @property
    def response(self):
        try:
            return self.inner.response
        except AttributeError:
            pass


class ValidationFailure(GetMediaError):
    status = 1001

    def __init__(self, url, expected_mime, detected_mime, content):
        self.expected_mime = expected_mime
        self.detected_mime = detected_mime
        self.content = content
        self.args = (expected_mime, detected_mime, content)
        self.message = "InvalidMIME expected '%s' found '%s' %s %s" % (
            expected_mime, detected_mime, url, self.status)


def get_media_wrapper(tup, cache_bad=False):
    "This calls get_media and handles all the failure scenarios"
    url, t, fmt = tup
    logger.debug("Starting   %s", url)

    def update_status(status):
        apidbpool.execute(
            "UPDATE media SET last_status=%s, last_check=now() WHERE url=%s",
            (status, url))

    try:
        get_media(url, t, fmt)
        logger.info("Success! %s %s %s 200", fmt, t, url)
        return 200
    except KeyboardInterrupt:
        raise
    except ValidationFailure as vf:
        update_status(vf.status)
        if cache_bad:
            write_bad(url, vf.content)
        logger.error(u"{0}".format(vf))
        return vf.status
    except GetMediaError as gme:
        update_status(gme.status)
        logger.error(u"{0}".format(gme))
        return gme.status
    except requests.exceptions.ConnectionError as connectione:
        logger.error(
            "ConnectionError %s %s %s 1503",
            url, connectione.errno, connectione.message)
        return 1503
    except Exception:
        update_status(1000)
        logger.exception("Unhandled error processing: %s 1000", url)
        return 1000


def get_media(url, t, fmt):
    if check_ignore_media(url):
        raise GetMediaError(url, 1002)

    try:
        response = rsess().get(url, timeout=REQ_CONNECT_TIMEOUT)
        response.raise_for_status()
    except requests.exceptions.HTTPError as httpe:
        raise ReqFailure(url, httpe.response.status_code, httpe)

    validator = get_validator(fmt)
    valid, detected_mime = validator(url, t, fmt, response.content)
    if not valid:
        if detected_mime == "text/html":
            logger.debug("Found - media is not valid and detected_mime is text/html")
            if "Access Denied" in response.content:
                logger.debug ("Found Access Denied. Trying to set status to 1403...")
                # Want to set status code to 1403
                raise ReqFailure(url, 1403, "Access Denied")
            else:
                raise ValidationFailure(url, fmt, detected_mime, response.content)
        else:
            raise ValidationFailure(url, fmt, detected_mime, response.content)
    logger.debug("Validated: %s %s %s %s", url, t, fmt, detected_mime)

    fobj = StringIO(response.content)
    try:
        mo = MediaObject.fromobj(
            fobj, type=t, mime=fmt, url=url,
            detected_mime=detected_mime)
    except UnknownMediaTypeError as umte:
        # This shouldn't happen given the above validation...
        raise ValidationFailure(url, fmt, umte.mime, response.content)

    try:
        stor = IDigBioStorage()
        k = mo.get_key(stor)
        if k.exists():
            logger.debug("Skipped  etag %s, already present", mo.etag)
        else:
            mo.upload(stor, fobj)
            logger.debug("Uploaded etag %s, already present")
        with PostgresDB() as idbmodel:
            mo.update_media(idbmodel)
            mo.ensure_object(idbmodel)
            mo.ensure_media_object(idbmodel)
            idbmodel.commit()
        return True
    except KeyboardInterrupt:
        raise
    except Exception as e:
        logger.exception("Error saving url: %s", url)
        status = 2000
        raise GetMediaError(url, status, inner=e)


def get_media_generator_filtered(prefix):
    sql = """
        SELECT url,type,mime
        FROM (
           SELECT media.url,type,mime,etag
           FROM media
           LEFT JOIN media_objects ON media.url = media_objects.url
           WHERE media.url LIKE %(urlfilter)s
             AND type IS NOT NULL
             AND (last_status IS NULL
                  OR (last_status >= 400 AND last_check < now() - %(interval)s::interval))
           ) AS a
        WHERE a.etag IS NULL"""
    url_rows = apidbpool.fetchall(
        sql, {'urlfilter': prefix + '%', 'interval': LAST_CHECK_INTERVAL},
        cursor_factory=cursor)
    logger.info("Found %d urls to check with prefix %r", len(url_rows), prefix)
    return url_rows


def get_media_generator():
    sql = """
        SELECT * FROM (
            SELECT substring(url from 'https?://[^/]*[/?]'), count(*)
            FROM (
                SELECT media.url, media_objects.etag
                FROM media
                LEFT JOIN media_objects ON media.url = media_objects.url
                WHERE type IS NOT NULL
                  AND (last_status IS NULL OR (last_status >= 400 and last_check < now() - %s::interval))

            ) AS a
            WHERE a.etag IS NULL
            GROUP BY substring(url from 'https?://[^/]*[/?]')
        ) AS b WHERE substring != ''
        ORDER BY count
    """

    subs_rows = apidbpool.fetchall(sql, (LAST_CHECK_INTERVAL,))
    logger.info("Found %d urlprefixes", len(subs_rows))
    for sub_row in subs_rows:
        subs = sub_row[0]
        for r in get_media_generator_filtered(subs):
            yield r

def get_media_consumer(prefix):
    logger.info("Starting get_media, prefix:%s", prefix)
    p = Pool(POOL_SIZE)
    count = 0
    counts = Counter()
    if prefix:
        urls = get_media_generator_filtered(prefix)
    else:
        urls = get_media_generator()

    for r in p.imap_unordered(get_media_wrapper, urls):
        counts[r] += 1
        count += 1

        if count % 1000 == 0:
            logger.info("Count: %8d; codecounts: %r", count, counts.most_common())
    logger.info("Count: %8d; codecounts: %r (Finished)", count, counts.most_common())
