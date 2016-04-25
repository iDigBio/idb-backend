from __future__ import division, absolute_import
from __future__ import print_function
from future_builtins import map, filter

import os
import cStringIO
import sys
import logging

from collections import Counter, namedtuple

from gevent.pool import Pool
from gevent import monkey
from PIL import Image
from boto.exception import S3ResponseError


from idb.config import ENV
from idb.helpers.memoize import memoized
from idb.helpers.storage import IDigBioStorage
from idb.postgres_backend import apidbpool, NamedTupleCursor
from idb.helpers.logging import idblogger


WIDTHS = {
    'thumbnail': 260,
    'webview': 600
}

POOLSIZE = 50

log = idblogger.getChild('deriv')


CheckItem = namedtuple(
    'CheckItem', ['etag', 'bucket', 'media', 'thumbnail', 'fullsize', 'webview'])

GenerateResult = namedtuple('GenerateResult', ['etag', 'items'])
GenerateItem = namedtuple('GenerateItem', ['key', 'data'])
CopyItem = namedtuple('CopyItem', ['key', 'data'])


class BadImageError(Exception):
    etag = None
    inner = None

    def __init__(self, message, inner=None):
        self.message = message
        self.inner = inner


def main(bucket):
    sql = ("SELECT etag, bucket FROM objects WHERE derivatives=false AND bucket=%s",
           (bucket,))
    objects = apidbpool.fetchall(*sql, cursor_factory=NamedTupleCursor)
    log.info("Checking derivatives for %d objects", len(objects))

    pool = Pool(POOLSIZE)
    check_items = pool.imap_unordered(get_keys, objects, maxsize=400)
    # this step produces the resized images: lots of mem, keep a choke on it.
    results = pool.imap_unordered(check_and_generate, check_items, maxsize=100)
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


def count_results(results, update_freq=100):
    c = Counter()
    count = 0

    def output():
        log.info("Checked:%6d  Generated:%6d  Existed:%6d  Erred:%6d",
                 count, c['generated'], c['existed'], c['erred'])

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
    s = get_store()
    bucketbase = u"idigbio-{0}-{1}".format(bucket, ENV)
    return CheckItem(unicode(etag), bucket,
                     s.get_key(etag, bucketbase),
                     s.get_key(etag + ".jpg", bucketbase + "-thumbnail"),
                     s.get_key(etag + ".jpg", bucketbase + "-fullsize"),
                     s.get_key(etag + ".jpg", bucketbase + "-webview"))


def check_and_generate(item):
    # check if thumbnail exists as proxy for everything existing
    if False and item.thumbnail.exists():
        log.debug("%s Thumbnail shortcut", item.etag)
        return GenerateResult(item.etag, [])

    img = None
    try:
        img = get_media_img(item.media)
    except S3ResponseError:
        return None
    except BadImageError as bie:
        log.error("%s: %s", item.etag, bie.message)
        return None

    try:
        items = generate_all(item, img)
        return GenerateResult(item.etag, items)
    except BadImageError as bie:
        log.error("%s: %s", item.etag, bie.message)
        return None
    except KeyboardInterrupt:
        raise
    except Exception:
        log.exception("%s: Failed generating", item.etag)
        return None


def generate_all(item, img):
    dtypes = ('fullsize', 'thumbnail', 'webview')
    derivs = [build_deriv(item, img, dtype) for dtype in dtypes]
    return list(filter(None, derivs))


def build_deriv(item, img, deriv):
    key = getattr(item, deriv)
    if key.exists():
        log.debug("%s: derivative exists", key)
        return

    if deriv == 'fullsize' and item.bucket == 'images' and img.format == 'JPEG':
        return CopyItem(item.fullsize, item.media)

    if deriv != 'fullsize':
        img = resize_image(img, deriv)
    buff = img_to_buffer(img, format='JPEG', quality=95)
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
    dervbuff = cStringIO.StringIO()
    img.save(dervbuff, **kwargs)
    dervbuff.seek(0)
    return dervbuff


def key_to_buffer(key):
    try:
        buff = cStringIO.StringIO()
        key.get_contents_to_file(buff)
        buff.seek(0)
        return buff
    except S3ResponseError as e:
        log.error("%r failed downloading with %r %s %s", key, e.status, e.reason, key.name)
        raise


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


def get_media_img(key):
    buff = key_to_buffer(key)
    try:
        if 'sounds' in key.bucket.name:
            log.debug("%s converting wave to img", key.name)
            return wave_to_img(buff)

        if 'images' in key.bucket.name:
            img = Image.open(buff)
            img.load()  # Make sure Pillow actually processes it
            if img.mode != 'RGB':
                img = img.convert('RGB')
            return img
        raise BadImageError(
            "Unknown mediatype in bucket {0!r}, expected images or sounds".format(
                key.bucket.name))

    except IOError as ioe:
        raise BadImageError("Error loading image {0!r}".format(ioe), inner=ioe)


def wave_to_img(buff):
    from idigbio_ingestion.lib.waveform import Waveform
    return Waveform(buff).generate_waveform_image()


if __name__ == '__main__':
    monkey.patch_all()
    logging.root.setLevel(logging.DEBUG)
    #logging.root.setLevel(logging.INFO)
    logging.getLogger('boto').setLevel(logging.INFO)
    logging.getLogger('requests').setLevel(logging.WARNING)

    if len(sys.argv) > 1:
        for bucket in sys.argv[1:]:
            main(bucket)
    else:
        print("""Usage:  derivatives.py <BUCKET ...>

    BUCKET can be any of {images, sounds}
        """, file=sys.stderr)
