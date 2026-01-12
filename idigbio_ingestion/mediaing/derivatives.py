from __future__ import division, absolute_import
from __future__ import print_function
#from future_builtins import map, filter

import io

from datetime import datetime
from collections import Counter, namedtuple
import itertools

from gevent.pool import Pool
from PIL import Image, ImageFile
from botocore.exceptions import ClientError


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
ImageFile.LOAD_TRUNCATED_IMAGES = True
# Some really large images starting to come from some data providers. Reduce
# the pool size to keep from running the workflow machine out of memory.
POOLSIZE = 10
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
        pool = gipcpool.Pool(size=procs)
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
    results = pool.imap_unordered(one, objects)
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
    return apidbpool.fetchall(sql, (buckets, DERIVATIVES_BLACKLIST,), cursor_factory=NamedTupleCursor)


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
    etag = str(etag)
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
    except (ClientError):
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
    deriv = first(DTYPES, key.bucket_name.endswith)
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
            if item is not None:
                IDigBioStorage.retry_loop(lambda: upload_item(item))
        return gr
    except (ClientError):
        logger.exception("%s failed uploading derivatives", gr.etag)
    except Exception:
        logger.exception("%s Unexpected error", gr.etag)


def upload_item(item, content_type="image/jpeg"):
    """
    Boto3/Python3 version.

    Expectations:
      - item.key is a boto3 s3.Object (destination)
          has: .bucket_name (str), .key (str), .put(), .upload_fileobj(), .copy_from()
      - if isinstance(item, CopyItem):
          item.data is a boto3 s3.Object (source)
            has: .bucket_name (str), .key (str)
        else:
          item.data is file-like (opened in 'rb') OR bytes-like
    """
    dst = item.key

    # Hard assertions so we don't silently do the wrong thing
    if not (hasattr(dst, "bucket_name") and hasattr(dst, "key")):
        raise TypeError(
            f"item.key must be boto3 s3.Object; got {type(dst)!r} "
            f"(attrs: {sorted(set(dir(dst)) & {'bucket_name','key','name','bucket'})})"
        )

    if isinstance(item, CopyItem):
        src = item.data

        if not (hasattr(src, "bucket_name") and hasattr(src, "key")):
            raise TypeError(
                f"CopyItem.data must be boto3 s3.Object; got {type(src)!r} "
                f"(attrs: {sorted(set(dir(src)) & {'bucket_name','key','name','bucket'})})"
            )

        logger.debug(
            "copying s3://%s/%s -> s3://%s/%s",
            src.bucket_name, src.key, dst.bucket_name, dst.key
        )

        # S3 requires REPLACE to change ContentType/metadata during copy
        dst.copy_from(
            CopySource={"Bucket": src.bucket_name, "Key": src.key},
            ContentType=content_type,
            MetadataDirective="REPLACE",
            Metadata={},  # add any x-amz-meta-* you need preserved/added
        )

    else:
        data = item.data
        logger.debug("uploading s3://%s/%s", dst.bucket_name, dst.key)

        # If it's file-like, rewind to be safe
        if hasattr(data, "seek"):
            try:
                data.seek(0)
            except Exception:
                pass

        # bytes-like upload
        if isinstance(data, (bytes, bytearray, memoryview)):
            dst.put(Body=bytes(data), ContentType=content_type)
        else:
            # file-like upload
            dst.upload_fileobj(data, ExtraArgs={"ContentType": content_type})



def img_to_buffer(img, **kwargs):
    kwargs.setdefault('format', 'JPEG')
    kwargs.setdefault('quality', 95)
    dervbuff = io.BytesIO()
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


def fetch_media(obj):
    """
    Download the object into memory and verify its MD5/ETag.
    """
    try:
        # obj.key is the object-name string
        return IDigBioStorage.get_contents_to_mem(obj, md5=obj.key)
    except ClientError as e:
        logger.error("%r failed downloading (%s)", obj, e)
        raise
    except ValueError:                                # raised on MD5 mismatch
        logger.error("%r failed downloading on md5 mismatch", obj)
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
