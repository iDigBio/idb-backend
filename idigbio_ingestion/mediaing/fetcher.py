from __future__ import division, absolute_import, print_function

import logging
import itertools
import re
import signal
from collections import Counter
from datetime import datetime

import requests
import gevent.pool
import gipc
from gevent import sleep
from boto.exception import BotoServerError, BotoClientError
from requests.exceptions import MissingSchema, InvalidSchema, InvalidURL, ConnectionError
from psycopg2.extensions import cursor


from idb.helpers import ilen
from idb.helpers.logging import idblogger
from idb.helpers.signals import signalcm
from idb.helpers.storage import IDigBioStorage
from idb.helpers.media_validation import MediaValidationError, sniff_mime

from idb.postgres_backend import apidbpool
from idb.postgres_backend.db import MediaObject, PostgresDB


from idigbio_ingestion.mediaing import IGNORE_PREFIXES, Status


logger = idblogger.getChild('mediaing')

LAST_CHECK_INTERVAL = '1 month'

# TODO: Add idb __version__ to this string?
USER_AGENT = 'iDigBio Media Ingestor (idigbio@acis.ufl.edu https://www.idigbio.org/wiki/index.php/Media_Ingestor)'


PREFIX_RE = re.compile('^https?://[^/]*[/?]')


def once(prefix=None, ignores=IGNORE_PREFIXES):
    "Fetch all outstanding media and exit"
    fetchitems = get_items(prefix=prefix)
    groups = group_by_prefix(fetchitems)
    procs = start_all_procs(groups).values()
    fetchitems = None
    groups = None
    logger.debug("%d procs started, waiting...", len(procs))
    for p in procs:
        p.join()


def start_all_procs(groups, running=None):
    if running is None:
        running = {}

    apidbpool.closeall()  # clean before proc fork
    for prefix, items in groups:
        if prefix in running:
            if prefix is None:
                # We can't disambiguate if we don't have a prefix; just
                # skip it until running[None] is empty again
                pass
            else:
                logger.critical("Trying to start second process for prefix %r", prefix)
                continue
        logger.debug("Starting subprocess for %s", prefix)
        running[prefix] = gipc.start_process(
            process_list, (items,), {'forprefix': prefix}, name="mediaing-{0}".format(prefix), daemon=False)
    return running


def continuous(prefix=None, looptime=3600):
    "Continuously loop checking for and downloading media"
    running = {}
    while True:
        logger.debug("Loop top")
        t1 = datetime.now()
        ignores = set(IGNORE_PREFIXES)
        for pf, proc in running.items():
            if proc.exitcode is not None:
                del running[pf]
                lvl = logging.CRITICAL if proc.exitcode != 0 else logging.DEBUG
                logger.log(lvl, "Process for %s failed with %s", pf, proc.exitcode)

        ignores = set(IGNORE_PREFIXES) | set(running.keys())
        ignores.discard(None)
        fetchitems = get_items(ignores=ignores, prefix=prefix)
        groups = group_by_prefix(fetchitems)
        running = start_all_procs(groups, running)
        fetchitems = None
        groups = None
        time = (datetime.now() - t1).total_seconds()
        logger.debug("Finished loop in %s", time)
        sleep(looptime - time)


def process_list(fetchitems, forprefix=''):
    """Process a list of FetchItems.

    This is intended to be the toplevel entry point of a subprocess
    working on a list of one domain's urls

    """
    try:
        store = IDigBioStorage()
        fetchrpool = gevent.pool.Pool(get_fetcher_count(forprefix))
        uploadpool = gevent.pool.Pool(8)
        items = fetchrpool.imap_unordered(lambda fi: fi.get_media(), fetchitems, maxsize=10)
        items = uploadpool.imap_unordered(lambda fi: fi.upload_to_storage(store), items, maxsize=10)
        items = itertools.imap(FetchItem.cleanup, items)
        items = count_result_types(items, forprefix=forprefix)
        return ilen(items)  # consume the generator
    except StandardError:
        logger.exception("Unhandled error forprefix:%s", forprefix)
        raise


def get_items(prefix=None, ignores=IGNORE_PREFIXES, last_check_interval=None):
    """Return the FetchItems that should be processed

    This takes into account ITEMCLASSES and will generate the
    appropriate subclass based on registered prefixes.

    """
    sql = """
        SELECT url, type, mime
        FROM media
        WHERE type IS NOT NULL
          AND (last_status IS NULL
               OR (last_status >= 400 AND last_check < now() - %s::interval))
    """
    params = []
    params.append(last_check_interval or LAST_CHECK_INTERVAL)
    if prefix:
        sql += "\n AND url LIKE %s"
        params.append(prefix + '%')
    else:
        for i in ignores:
            assert i, "Can't ignore {0!r}".format(i)
            sql += "\n AND url NOT LIKE %s"
            params.append(i + '%')
    sql += "\n ORDER BY url"
    logger.debug("Querying %r", apidbpool.mogrify(sql, params))
    url_rows = apidbpool.fetchall(sql, params, cursor_factory=cursor)
    logger.info("Found %d urls to check", len(url_rows))

    for r in url_rows:
        m = PREFIX_RE.search(r[0])
        prefix = m and m.group()
        cls = ITEMCLASSES.get(prefix, FetchItem)
        yield cls(*r, prefix=prefix)


def group_by_prefix(items):
    #force list creation, otherwise itertools skips elements to find the
    #next group key before the worker has time to go through it
    return [(prefix, list(r)) for prefix, r in itertools.groupby(items, lambda x: x.prefix)]


def count_result_types(results, interval=1000, forprefix=''):
    counts = Counter()
    count = 0
    output = lambda: logger.info("%s Count: %6d; codecounts: %r", forprefix, count, counts.most_common())
    with signalcm(signal.SIGUSR1, lambda s, f: output()):
        for count, r in enumerate(results, 1):
            counts[r.status_code] += 1
            if count % interval == 0:
                output()
            yield r
    logger.info("%s Count: %8d; codecounts: %r FINISHED!", forprefix, count, counts.most_common())


ITEMCLASSES = {}

def prefix(prefix):
    "Register class as the FetchItem type for given prefix"
    assert PREFIX_RE.search(prefix).group() == prefix, \
        "Specified prefix must match PREFIX_RE"

    def wrapper(cls):
        ITEMCLASSES[prefix] = cls
        setattr(cls, 'PREFIX', prefix)
        return cls
    return wrapper


def get_fetcher_count(prefix):
    return ITEMCLASSES.get(prefix, FetchItem).FETCHER_COUNT


class FetchItem(object):
    """Helper to track a Media entry being fetched, verified and uploaded to storage

    Behavior can be overrided for a specific prefix by subclassing and
    attaching the @prefix("...") decorator

    """

    #: Timeout, in sec, of both connect and read
    REQ_TIMEOUT = 15.5

    #: Number of concurrent connections allowed
    FETCHER_COUNT = 2

    #: Can http connections be reuesed?
    REUSE_CONN = True

    #: class (static) variable for the session to use
    session = None

    url = None
    type = None
    mime = None

    status_code = None
    response = None
    reason = None

    media_object = None

    def __init__(self, url, type, mime, prefix=None, status_code=None):
        self.url = url
        self.type = type
        self.mime = mime
        if not prefix:
            m = PREFIX_RE.search(url)
            prefix = m and m.group()
        self.prefix = prefix
        if status_code:
            self.status_code = status_code

    def __repr__(self):
        return "{0}({1!r}, {2!r}, {3!r}, status_code={4!s})".format(
            self.__class__.__name__, self.url, self.type, self.mime, self.status_code)

    @property
    def ok(self):
        return self.status_code == Status.OK

    @property
    def content(self):
        return self.response.content

    def get_media(self):
        "This calls get_media and handles all the failure scenarios"
        try:
            self.fetch()
            self.validate()
            if self.ok:
                logger.info("Success!  %s", self.url)
            return self
        except StandardError:
            self.status_code = Status.UNHANDLED_FAILURE
            logger.exception("Unhandled error processing: %s", self.url)
            return self

    def fetch(self):
        logger.debug("Starting  %s", self.url)
        try:
            self.response = self._get()
            self.reason = self.response.reason
        except (MissingSchema, InvalidSchema, InvalidURL) as mii:
            self.reason = str(mii)
            self.status_code = Status.UNREQUESTABLE
        except ConnectionError as connectione:
            self.reason = "{0} {1}".format(connectione.errno, connectione.message)
            self.status_code = Status.CONNECTION_ERROR
        else:
            try:
                self.status_code = Status(self.response.status_code)
            except ValueError:
                # Status isn't an exhaustive list so it's possible the
                # lookup failed; we'll deal with them as they pop up.
                logger.critical("Failed finding Status(%r), reason '%s' aborting %s",
                                self.response.status_code, self.reason, self.url)
                self.status_code = Status.UNKNOWN

        if not self.ok:
            logger.error("Failed!   %s '%s' %r", self.url, self.reason, self.status_code)
        return self

    @classmethod
    def get_session(cls):
        if not cls.session:
            cls.session = s = requests.Session()
            # http://urllib3.readthedocs.io/en/latest/helpers.html#module-urllib3.util.retry
            retry = requests.adapters.Retry(total=10, connect=2, read=3, backoff_factor=5)

            adapter = requests.adapters.HTTPAdapter(
                max_retries=retry, pool_block=True, pool_maxsize=cls.FETCHER_COUNT)

            s.mount('http://', adapter)
            s.mount('https://', adapter)
            s.headers['User-Agent'] = USER_AGENT
            if not cls.REUSE_CONN:
                s.headers['connection'] = 'close'

        return cls.session

    def _get(self):
        "Run the actual HTTP get"
        return self.get_session().get(self.url, timeout=self.REQ_TIMEOUT)

    def validate(self):
        if not self.ok:
            return self
        try:
            self.media_object = MediaObject.frombuff(
                self.content,
                url=self.url, type=self.type, mime=self.mime)
            logger.debug("Validated %s %s %s", self.url, self.type, self.mime)
            return self
        except MediaValidationError as mve:
            self.status_code = Status.VALIDATION_FAILURE
            self.reason = str(mve)

        detected_mime = sniff_mime(self.content)
        logger.error("Invalid!  %s %s", self.url, self.reason)

        if detected_mime in ('text/html', 'text/plain'):
            sc = inspect_html_response(self.content)
            if sc:
                self.status_code = sc
                logger.error("HtmlResp  %s %r", self.url, sc)
        return self

    def upload_to_storage(self, store, attempt=1):
        if not self.ok:
            return self
        try:
            mo = self.media_object
            k = mo.get_key(store)
            if k.exists():
                logger.debug("NoUpload  %s etag %s, already present", self.url, mo.etag)
                return self
            try:
                mo.upload(store, self.content)
                logger.debug("Uploaded  %s etag %s", self.url, mo.etag)
            except (BotoServerError, BotoClientError) as e:
                logger.exception("Failed uploading to storage: %s", self.url)
                self.reason = str(e)
                self.status_code = Status.STORAGE_ERROR
                return self

            with PostgresDB() as idbmodel:
                mo.ensure_object(idbmodel)
                mo.ensure_media_object(idbmodel)
                try:
                    mo.last_status = self.status_code.value
                except AttributeError:
                    mo.last_status = self.status_code
                # Don't need to ensure_media, we're processing through
                # media entries, just update the last_check and status
                mo.update_media(idbmodel)
                idbmodel.commit()
        except Exception:
            logger.exception("DbSaveErr %s", self.url)
            self.status_code = Status.STORAGE_ERROR
        return self

    def cleanup(self):
        "Get rid of all the big references, hang on to url and status_code"
        if self.response is not None:
            del self.response
            self.response = None
        if self.media_object is not None:
            del self.media_object
            self.media_object = None
        self.reason = None
        return self

    def __del__(self):
        self.cleanup()



DENIED_RE = re.compile("access denied", re.I)
BLOCKED_RE = re.compile("ip (?:address)? (blocked|blacklisted)")

def inspect_html_response(content):
    content = content.lower()
    if DENIED_RE.search(content):
        return Status.FAUX_DENIED
    if BLOCKED_RE.search(content):
        return Status.BLOCKED


@prefix("http://www.tropicos.org/")
class TropicosItem(FetchItem):
    #All sleep counts are in seconds
    sleep_blacklist = 49 * 60
    sleep_retry = 24
    sleep_unavailable = 360
    sleep_notfound = 2
    retries = 4

    #: in sec, both connect and read timeout
    REQ_TIMEOUT = 3 * 60 + 1

    FETCHER_COUNT = 1
    REUSE_CONN = False

    def get_media(self):
        try:
            while True:
                super(TropicosItem, self).get_media()
                self.retries -= 1
                if self.ok:
                    return self
                if self.retries <= 0:
                    logger.error("NoRetries %s", self.url)
                    return self
                if self.status_code in (Status.FORBIDDEN, Status.NOT_FOUND, Status.FAUX_DENIED):
                    logger.debug("Sleeping  %s for %ss, retries:%s",
                                 self.url, self.sleep_notfound, self.retries)
                    sleep(self.sleep_notfound)
                    return self

                if self.status_code in (Status.SERVICE_UNAVAILABLE, ):
                    logger.debug("Sleeping  %s for %ss, retries:%s (unavailable)",
                                 self.url, self.sleep_unavailable, self.retries)
                    sleep(self.sleep_unavailable)
                elif self.status_code == Status.BLOCKED:
                    logger.debug("Sleeping  %s for %ss, retries:%s (blocked)",
                                 self.url, self.sleep_blacklist, self.retries)
                    sleep(self.sleep_blacklist)
                else:
                    logger.debug("Sleeping  %s for %ss, retries:%s",
                                 self.url, self.sleep_retry, self.retries)
                    sleep(self.sleep_retry)
                    self.sleep_retry *= 1.5
        except StandardError:
            self.status_code = Status.UNHANDLED_FAILURE
            logger.exception("Unhandled %s", self.url)
        return self


@prefix("http://media.idigbio.org/")
class OldMediaApiItem(FetchItem):
    FETCHER_COUNT = 16
    etag = None

    def get_media(self):
        try:
            self.etag = MediaObject.oldmediaapietag(self.url)
            self.media_object = MediaObject(url=self.url, etag=self.etag,
                                            type=self.type, bucket=self.type,
                                            mime=self.mime, detected_mime=self.mime)
            self.status_code = Status.OK
        except IndexError:
            logger.exception("What?")
            self.status_code = Status.UNHANDLED_FAILURE
        return self


@prefix("http://arctos.database.museum/")
class ArctosItem(FetchItem):
    FETCHER_COUNT = 1
    REUSE_CONN = False
