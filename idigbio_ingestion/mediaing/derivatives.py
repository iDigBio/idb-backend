from __future__ import division, absolute_import
from __future__ import print_function
from future_builtins import map, filter

import cStringIO

from datetime import datetime
from collections import Counter, namedtuple

import gevent
from gevent.pool import Pool
from PIL import Image
from boto.exception import S3ResponseError, S3DataError


from idb.helpers import first
from idb.helpers.memoize import memoized
from idb import __version__
from idb import config
from idb.helpers.storage import IDigBioStorage
from idb.postgres_backend import apidbpool, NamedTupleCursor
from idb.helpers.logging import idblogger


WIDTHS = {
    'thumbnail': 260,
    'webview': 600
}

POOLSIZE = 50
DTYPES = ('thumbnail', 'fullsize', 'webview')

log = idblogger.getChild('deriv')

CheckItem = namedtuple(
    'CheckItem', ['etag', 'bucket', 'media', 'keys'])

GenerateResult = namedtuple('GenerateResult', ['etag', 'items'])
GenerateItem = namedtuple('GenerateItem', ['key', 'data'])
CopyItem = namedtuple('CopyItem', ['key', 'data'])


class BadImageError(Exception):
    etag = None
    inner = None

    def __init__(self, message, inner=None):
        self.message = message
        self.inner = inner


def continuous(buckets):
    "Continuously run the main loop, checking for derivatives"
    log.info("Starting up continuous operation, version: %s", __version__)
    minlooptime = 600  # 10minutes

    while True:
        t1 = datetime.now()
        main(buckets)
        log.info("Completed complete derivatives run in %s", (datetime.now() - t1))
        sleeptime = minlooptime - (datetime.now() - t1).total_seconds()
        if sleeptime:
            gevent.sleep(max([sleeptime, 1]))


def main(buckets, run_migrate=True):
    if not buckets:
        buckets = ('images', 'sounds')
    if run_migrate:
        migrate()
    objects = get_objects(buckets)
    log.info("Checking derivatives for %d objects", len(objects))

    pool = Pool(POOLSIZE)
    check_items = pool.imap_unordered(get_keys, objects)
    check_items = pool.imap_unordered(check_all, check_items)
    results = pool.imap_unordered(generate_all, check_items)
    results = pool.imap_unordered(upload_all, results, maxsize=100)
    results = count_results(results, update_freq=100)
    etags = ((gr.etag,) for gr in results if gr)

    count = apidbpool.executemany(
        "UPDATE objects SET derivatives=true WHERE etag = %s",
        etags,
        autocommit=True
    )
    log.info("Updated %s records", count)
    pool.join(raise_error=True)


def get_objects(buckets):
    assert isinstance(buckets, (tuple, list))
    sql = """SELECT etag, bucket
             FROM objects
             WHERE derivatives=false AND bucket IN %s
             ORDER BY random()
    """
    return apidbpool.fetchall(sql, (buckets,), cursor_factory=NamedTupleCursor)


def count_results(results, update_freq=100):
    start = datetime.now()
    c = Counter()
    count = 0

    def output():
        rate = count / max([(datetime.now() - start).total_seconds(), 1])
        log.info("Checked:%6d  Generated:%6d  Existed:%6d  Erred:%6d Rate:%6.1f/s",
                 count, c['generated'], c['existed'], c['erred'], rate)

    try:
        for count, result in enumerate(results, 1):
            if result is None:
                c['erred'] += 1
            elif len(result.items) > 0:
                c['generated'] += 1
            else:
                c['existed'] += 1

            if count % update_freq == 0:
                output()
            yield result
    except KeyboardInterrupt:
        output()
        raise
    output()


get_store = memoized()(lambda: IDigBioStorage())

def get_keys(obj):
    etag, bucket = obj.etag, obj.bucket
    etag = unicode(etag)
    s = get_store()
    bucketbase = u"idigbio-{0}-{1}".format(bucket, config.ENV)
    mediakey = s.get_key(etag, bucketbase)
    keys = [s.get_key(etag + ".jpg", bucketbase + '-' + dtype) for dtype in DTYPES]
    return CheckItem(etag, bucket, mediakey, keys)


def check_key(k):
    if k.exists():
        log.debug("%s: derivative exists", k)
        return False
    return True


def check_all(item):
    keys = filter(check_key, item.keys)
    return CheckItem(item.etag, item.bucket, item.media, list(keys))


def generate_all(item):
    if len(item.keys) == 0:
        return GenerateResult(item.etag, [])

    img = None
    try:
        buff = fetch_media(item.media)
        img = convert_media(item, buff)
    except (S3ResponseError, S3DataError):
        return None
    except BadImageError as bie:
        log.error("%s: %s", item.etag, bie.message)
        return None

    try:
        items = map(lambda k: build_deriv(item, img, k), item.keys)
        return GenerateResult(item.etag, list(items))
    except BadImageError as bie:
        log.error("%s: %s", item.etag, bie.message)
        return None
    except KeyboardInterrupt:
        raise
    except Exception:
        log.exception("%s: Failed generating", item.etag)
        return None


def build_deriv(item, img, key):
    deriv = first(DTYPES, key.bucket.name.endswith)
    assert deriv
    if deriv == 'fullsize' and item.bucket == 'images' and img.format == 'JPEG':
        return CopyItem(key, item.media)
    if deriv != 'fullsize':
        img = resize_image(img, deriv)
    buff = img_to_buffer(img)
    return GenerateItem(key, buff)


def upload_all(gr):
    if not gr:
        return
    try:
        for item in gr.items:
            upload_item(item)
        return gr
    except S3ResponseError:
        log.exception("%s failed uploading derivatives", gr.etag)
    except KeyboardInterrupt:
        raise
    except:
        log.exception("Unexpected error")

def upload_item(item):
    key = item.key
    data = item.data
    if isinstance(item, CopyItem):
        log.debug("%s copying from bucket %s", key, data.bucket.name)
        data.copy(dst_bucket=key.bucket,
                  dst_key=key.name,
                  metadata={'Content-Type': 'image/jpeg'})
    else:
        # no key exists check here, that was done in build_deriv
        log.debug("%s uploading", key)
        key.set_metadata('Content-Type', 'image/jpeg')
        key.set_contents_from_file(data)
    key.make_public()


def img_to_buffer(img, **kwargs):
    kwargs.setdefault('format', 'JPEG')
    kwargs.setdefault('quality', 95)
    dervbuff = cStringIO.StringIO()
    img.save(dervbuff, **kwargs)
    dervbuff.seek(0)
    return dervbuff


def resize_image(img, deriv):
    derivative_width = WIDTHS[deriv]
    if img.size[0] > derivative_width:
        derivative_width_percent = (derivative_width / float(img.size[0]))
        derivative_horizontal_size = int(
            (float(img.size[1]) * float(derivative_width_percent)))
        try:
            return img.resize(
                (derivative_width, derivative_horizontal_size), Image.BILINEAR)
        except IOError as ioe:
            raise BadImageError("Error resizing {0}".format(ioe), inner=ioe)
    else:
        return img


def fetch_media(key):
    try:
        return IDigBioStorage.get_contents_to_mem(key, md5=key.name)
    except S3ResponseError as e:
        log.error("%r failed downloading with %r %s %s", key, e.status, e.reason, key.name)
        raise
    except S3DataError as e:
        log.error("%r failed downloading on md5 mismatch", key)
        raise

def convert_media(item, buff):
    try:
        if 'sounds' == item.bucket:
            log.debug("%s converting wave to img", item.etag)
            return wave_to_img(buff)

        if 'images' in item.bucket:
            return load_img(buff)
        raise BadImageError(
            "Unknown mediatype in bucket {0!r}, expected images or sounds".format(
                item.bucket))

    except IOError as ioe:
        raise BadImageError("Error loading image {0!r}".format(ioe), inner=ioe)


def wave_to_img(buff):
    from idigbio_ingestion.lib.waveform import Waveform
    img = Waveform(buff).generate_waveform_image()
    return img

def load_img(buff):
    img = Image.open(buff)
    img.load()  # Make sure Pillow actually processes it
    if img.mode != 'RGB':
        img = img.convert('RGB')
    return img

def migrate():
    log.info("Checking for objects in the old media api")
    with apidbpool.cursor() as cur:
        sql = """INSERT INTO objects (bucket, etag)
              (SELECT DISTINCT
                type,
                a.etag
              FROM idb_object_keys AS a
              LEFT JOIN objects AS b
                ON a.etag = b.etag
              WHERE b.etag IS NULL);
        """
        cur.execute(sql)
        rc = cur.rowcount
        log.info("Objects Migrated: %s", rc)
        sql = """INSERT INTO media (url, type, owner, last_status, last_check)
              (SELECT
                idb_object_keys.lookup_key,
                idb_object_keys.type,
                idb_object_keys.user_uuid::uuid,
                200,
                now()
              FROM idb_object_keys
              LEFT JOIN media
                ON lookup_key = url
              WHERE media.url IS NULL);
        """
        cur.execute(sql)
        rc = cur.rowcount
        log.info("Media Migrated: %s", rc)
        sql = """
            INSERT INTO media_objects (url, etag, modified)
              (SELECT
                idb_object_keys.lookup_key,
                idb_object_keys.etag,
                idb_object_keys.date_modified
              FROM idb_object_keys
              LEFT JOIN media_objects
                ON lookup_key = url
                AND media_objects.etag = idb_object_keys.etag
              WHERE media_objects.url IS NULL)
        """
        cur.execute(sql)
        rc = cur.rowcount
        log.info("Media Objects Migrated: %s", rc)
