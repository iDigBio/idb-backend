from __future__ import absolute_import
from gevent import monkey
monkey.patch_all()

from cStringIO import StringIO
from collections import Counter

import magic
import requests
from psycopg2.extensions import cursor
from gevent.pool import Pool

from idb.helpers.memoize import memoized
from idb.postgres_backend import apidbpool
from idb.postgres_backend.db import MediaObject, PostgresDB

from idb.helpers.storage import IDigBioStorage
from idb.helpers.media_validation import UnknownMediaTypeError, get_validator
from idb.helpers.conversions import get_accessuri, get_media_type

from idigbio_ingestion.lib.log import logger

POOL_SIZE = 5
LAST_CHECK_INTERVAL = '1 month'
REQ_CONNECT_TIMEOUT = 15 # seconds

USER_AGENT = 'iDigBio Media Ingestor (idigbio@acis.ufl.edu https://www.idigbio.org/wiki/index.php/Media_Ingestor)'

IGNORE_PREFIXES = [
    "http://media.idigbio.org/",
    "http://firuta.huh.harvard.edu/",
    "http://www.tropicos.org/"
]




@memoized()
def rsess():
    s = requests.Session()
    adapter = requests.adapters.HTTPAdapter(pool_connections=POOL_SIZE*2, pool_maxsize=POOL_SIZE*2)
    s.mount('http://', adapter)
    s.mount('https://', adapter)
    s.headers['User-Agent'] = USER_AGENT
    return s


def check_ignore_media(url):
    for p in IGNORE_PREFIXES:
        if url.startswith(p):
            return True
    return False


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
        mo = MediaObject.fromobj(fobj, filereference=url)
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
            mo.update_media(idbmodel, status=200)
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


def write_urls_to_db(media_urls):
    logger.info("Start Inserts")
    inserted_urls = set()

    scanned = 0
    to_insert, to_update = [], []
    itersql = """SELECT type,data
                 FROM idigbio_uuids_data
                 WHERE type='mediarecord' and deleted=false"""

    for r in apidbpool.fetchiter(itersql, named=True):
        scanned += 1
        url = get_accessuri(r["type"], r["data"])["accessuri"]
        o = get_media_type(r["type"], r["data"])
        form = o["format"]
        t = o["mediatype"]

        if url is not None:
            url = url.replace("&amp;", "&").strip()
            if not check_ignore_media(url):
                if url in media_urls:
                    # We're going to change something, but only if we're
                    # adding/replacing things, not nulling existing values.
                    if not (t, form) == media_urls[url] and form is not None and (t is not None or media_urls[url][0] is None):
                        to_update.append((t, form, url))
                elif url not in inserted_urls:
                    to_insert.append((url, t, form))
                    inserted_urls.add(url)

        if scanned % 100000 == 0:
            logger.info("Inserting: %8d, Updating: %8d, Scanned: %8d",
                        len(to_insert), len(to_update), scanned)
    with apidbpool.cursor() as cur:
        cur.executemany("INSERT INTO media (url,type,mime) VALUES (%s,%s,%s)", to_insert)
        cur.executemany("UPDATE media SET type=%s, mime=%s, last_status=NULL, last_check=NULL WHERE url=%s",
                        to_update)

    logger.info("Inserting: %8d, Updating: %8d, Scanned: %8d (Finished)",
                len(to_insert), len(to_update), scanned)


def get_postgres_media_urls(urlfilter=None):
    media_urls = dict()
    logger.info("Get Media URLs, urlfilter: %r", urlfilter)
    sql = "SELECT url,type,mime FROM media"
    params = []
    if urlfilter:
        sql += "\nWHERE url LIKE %s"
        params.append(urlfilter)

    for r in apidbpool.fetchiter(sql, params, cursor_factory=cursor):
        media_urls[r[0]] = (r[1], r[2])
    logger.info("Found %d urls already in DB", len(media_urls))
    return media_urls


def get_postgres_media_objects(urlfilter):
    assert urlfilter is None, "urlfilter isn't implemented on this function"
    count, rowcount, lrc = 0, 0, 0
    sql = "SELECT lookup_key, etag, date_modified FROM idb_object_keys"
    with apidbpool.connection() as insertconn:
        with insertconn.cursor(cursor_factory=cursor) as cur:
            for r in apidbpool.fetchiter(sql, name="get_postgres_media_objects"):
                cur.execute("""
                     INSERT INTO media_objects (url, etag, modified)
                     SELECT %(url)s, %(etag)s, %(modified)s
                     WHERE EXISTS (SELECT 1 FROM media WHERE url=%(url)s)
                       AND EXISTS (SELECT 1 FROM objects WHERE etag=%(etag)s)
                       AND NOT EXISTS (SELECT 1 FROM media_objects WHERE url=%(url)s AND etag=%(etag)s)
                """, {"url": r[0], "etag": r[1], "modified": r[2]})
                count += 1
                rowcount += cur.rowcount

                if rowcount != lrc and rowcount % 10000 == 0:
                    insertconn.commit()
                    logger.info("Count: %8d,  rowcount: %8d", count, rowcount)
                    lrc = rowcount
    insertconn.commit()
    logger.info("Count: %8d,  rowcount: %8d", count, rowcount)


def get_objects_from_ceph():
    existing_objects = set(
        r[0] for r in apidbpool.fetchiter("SELECT etag FROM objects", cursor_factory=cursor))

    logger.info("Found %d objects", len(existing_objects))

    s = IDigBioStorage()
    buckets = ["datasets", "images"]
    count = 0
    rowcount = 0
    lrc = 0
    with apidbpool.connection() as conn:
        with apidbpool.cursor() as cur:
            for b_k in buckets:
                b = s.get_bucket("idigbio-" + b_k + "-prod")
                for k in b.list():
                    if k.name not in existing_objects:
                        try:
                            ks = k.get_contents_as_string(headers={'Range': 'bytes=0-100'})
                            detected_mime = magic.from_buffer(ks, mime=True)
                            cur.execute(
                                """INSERT INTO objects (bucket,etag,detected_mime)
                                   SELECT %(bucket)s,%(etag)s,%(dm)s
                                   WHERE NOT EXISTS(
                                      SELECT 1 FROM objects WHERE etag=%(etag)s)""",
                                {"bucket": b_k, "etag": k.name, "dm": detected_mime})
                            existing_objects.add(k.name)
                            rowcount += cur.rowcount
                        except:
                            logger.exception("Ceph Error; bucket:%s keyname:%s", b_k, k.name)
                    count += 1

                    if rowcount != lrc and rowcount % 10000 == 0:
                        logger.info("Count: %8d,  rowcount: %8d", count, rowcount)

                        conn.commit()
                        lrc = rowcount
                conn.commit()
                logger.info("Count: %8d,  rowcount: %8d  (Finished %s)", count, rowcount, b_k)


def get_media_generator_filtered(urlfilter):
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
        sql, {'urlfilter': urlfilter, 'interval': LAST_CHECK_INTERVAL},
        cursor_factory=cursor)
    logger.info("Found %d urls to check for filter %r", len(url_rows), urlfilter)
    return url_rows


def get_media_generator(urlfilter=None):
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
        for r in get_media_generator_filtered(subs + '%'):
            yield r

def get_media_consumer(urlfilter):
    logger.info("Starting get_media, urlfilter:%s", urlfilter)
    p = Pool(POOL_SIZE)
    count = 0
    counts = Counter()
    if urlfilter:
        urls = get_media_generator_filtered(urlfilter)
    else:
        urls = get_media_generator()

    for r in p.imap_unordered(get_media_wrapper, urls):
        counts[r] += 1
        count += 1

        if count % 1000 == 0:
            logger.info("Count: %8d; codecounts: %r", count, counts.most_common())
    logger.info("Count: %8d; codecounts: %r (Finished)", count, counts.most_common())


# SQL Queries to import from old table:
#insert into media (url,type,owner) (select lookup_key,type,user_uuid::uuid from (select media.url, idb_object_keys.lookup_key, idb_object_keys.type, idb_object_keys.user_uuid from idb_object_keys left join media on lookup_key=url) as a where url is null);
#insert into objects (bucket,etag) (select type,etag from (select lookup_key, type, a.etag, b.etag as n from idb_object_keys as a left join objects as b on a.etag=b.etag) as c where n is null);
#insert into media_objects (url,etag,modified) (select lookup_key,etag,date_modified from (select media_objects.url, idb_object_keys.lookup_key, idb_object_keys.etag, idb_object_keys.date_modified from idb_object_keys left join media_objects on lookup_key=url and media_objects.etag=idb_object_keys.etag) as a where url is null);
