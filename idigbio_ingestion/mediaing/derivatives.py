from __future__ import division, absolute_import
from __future__ import print_function
from future_builtins import map, filter

import cStringIO

from datetime import datetime
from collections import Counter, namedtuple
import itertools

from gevent.pool import Pool
from PIL import Image
from boto.exception import BotoServerError, BotoClientError, S3DataError


from idb.helpers import first, gipcpool, ilen, grouper
from idb.helpers.memoize import memoized
from idb import config
from idb.helpers.storage import IDigBioStorage
from idb.postgres_backend import apidbpool, NamedTupleCursor
from idb.helpers.logging import idblogger
from idb.blacklists.derivatives import DERIVATIVES_BLACKLIST

WIDTHS = {
    'thumbnail': 260,
    'webview': 600
}

# Some really large images starting to come from some data providers. Reduce
# the pool size to keep from running the workflow machine out of memory.
POOLSIZE = 25
DTYPES = ('thumbnail', 'fullsize', 'webview')

logger = idblogger.getChild('deriv')

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


def main(buckets, procs=2):
    if not buckets:
        buckets = ('images', 'sounds')
    objects = objects_for_buckets(buckets)


    t1 = datetime.now()
    logger.info("Checking derivatives for %d objects", len(objects))

    if procs > 1:
        apidbpool.closeall()
        pool = gipcpool.Pool(procs)
        c = ilen(pool.imap_unordered(process_objects, grouper(objects, 1000)))
        logger.debug("Finished %d subprocesses", c)
    else:
        process_objects(objects)
    logger.info("Completed derivatives run in %s", (datetime.now() - t1))


def process_etags(etags):
    # Do not use DERIVATIVES_BLACKLIST here, this function is only
    # called from cli with specified human-provided etags.
    objects = objects_for_etags(etags)
    t1 = datetime.now()
    logger.info("Checking derivatives for %d objects", len(objects))
    process_objects(objects)
    logger.info("Completed derivatives run in %s", (datetime.now() - t1))


def process_objects(objects):
    pool = Pool(POOLSIZE)

    def one(o):
        logger.info("%s starting processing", o.etag)
        ci = get_keys(o)
        gr = generate_all(ci)
        return upload_all(gr)
    results = pool.imap_unordered(one, itertools.ifilter(None, objects))
    results = count_results(results, update_freq=100)
    etags = ((gr.etag,) for gr in results if gr)
    count = apidbpool.executemany(
        "UPDATE objects SET derivatives=true WHERE etag = %s",
        etags,
        autocommit=True
    )
    logger.info("Updated %s records", count)
    pool.join(raise_error=True)


def objects_for_buckets(buckets):
    assert isinstance(buckets, (tuple, list))
    sql = """SELECT etag, bucket
             FROM objects
             WHERE derivatives=false AND bucket IN %s
             AND etag NOT IN %s
             ORDER BY random()
    """
    return apidbpool.fetchall(sql, (buckets,DERIVATIVES_BLACKLIST,), cursor_factory=NamedTupleCursor)


def objects_for_etags(etags):
    assert isinstance(etags, (tuple, list))
    sql = """SELECT etag, bucket
             FROM objects
             WHERE derivatives=false AND etag IN %s
    """
    return apidbpool.fetchall(sql, (etags,), cursor_factory=NamedTupleCursor)


def count_results(results, update_freq=100):
    start = datetime.now()
    c = Counter()
    count = 0

    def output():
        rate = count / max([(datetime.now() - start).total_seconds(), 1])
        logger.info("Checked:%6d  Generated:%6d  Existed:%6d  Erred:%6d Rate:%6.1f/s",
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


def generate_all(item):
    if len(item.keys) == 0:
        return GenerateResult(item.etag, [])

    img = None
    try:
        buff = fetch_media(item.media)
        img = convert_media(item, buff)
    except (BotoServerError, BotoClientError):
        return None
    except BadImageError as bie:
        logger.error("%s caused BadImageError: %s", item.etag, bie.message)
        return None
    # catch PIL.Image.DecompressionBombError and other exceptions here
    except Exception as e:
        logger.error("%s caused Exception: %s", item.etag, e.message)
        return None



    try:
        items = map(lambda k: build_deriv(item, img, k), item.keys)
        return GenerateResult(item.etag, list(items))
    except BadImageError as bie:
        logger.error("%s: %s", item.etag, bie.message)
        return None
    except KeyboardInterrupt:
        raise
    except Exception:
        logger.exception("%s: Failed generating", item.etag)
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
            IDigBioStorage.retry_loop(lambda: upload_item(item))
        return gr
    except (BotoServerError, BotoClientError):
        logger.exception("%s failed uploading derivatives", gr.etag)
    except Exception:
        logger.exception("%s Unexpected error", gr.etag)


def upload_item(item):
    key = item.key
    data = item.data
    if isinstance(item, CopyItem):
        logger.debug("%s copying from bucket %s", key, data.bucket.name)
        data.copy(dst_bucket=key.bucket,
                  dst_key=key.name,
                  metadata={'Content-Type': 'image/jpeg'})
    else:
        # no key exists check here, that was done in build_deriv
        logger.debug("%s uploading", key)
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
    except BotoServerError as e:
        logger.error("%r failed downloading with %r %s %s", key, e.status, e.reason, key.name)
        raise
    except S3DataError as e:
        logger.error("%r failed downloading on md5 mismatch", key)
        raise
    except BotoClientError as e:
        logger.exception("%r failed downloading because...")
        raise

def convert_media(item, buff):
    try:
        if 'sounds' == item.bucket:
            logger.debug("%s converting wave to img", item.etag)
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
