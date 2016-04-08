from __future__ import division, absolute_import
from __future__ import print_function, unicode_literals
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
from idb.helpers.storage import IDigBioStorage
from idb.postgres_backend import apidbpool, NamedTupleCursor
from idigbio_ingestion.lib.log import getIDigBioLogger


WIDTHS = {
    'thumbnail': 260,
    'webview': 600
}

POOLSIZE = 50

log = getIDigBioLogger('derivatives')


CheckItem = namedtuple(
    'CheckItem', ['etag', 'bucket', 'media', 'thumbnail', 'fullsize', 'webview'])

GenerateResult = namedtuple('GenerateResult', ['etag', 'items'])
GenerateItem = namedtuple('GenerateItem', ['key', 'data'])
CopyItem = namedtuple('CopyItem', ['key', 'data'])


def main(bucket):
    sql = ("SELECT etag, bucket FROM objects WHERE derivatives=false AND bucket=%s",
           (bucket,))
    objects = apidbpool.fetchall(*sql, cursor_factory=NamedTupleCursor)
    log.info("Checking derivatives for %d objects", len(objects))

    pool = Pool(POOLSIZE)
    check_items = pool.imap_unordered(get_keys, objects, maxsize=1000)
    # this step produces the resized images: lots of mem, keep a choke on it.
    results = pool.imap_unordered(check_and_generate, check_items, maxsize=500)
    results = pool.imap_unordered(upload_all, results)
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
    for count, result in enumerate(results):
        if result is None:
            c['erred'] += 1
        elif len(result.items) > 0:
            c['generated'] += 1
        else:
            c['existed'] += 1

        if count % update_freq == 0:
            log.info("Generated: %6d  Existed:%6d  Erred: %6d",
                     c['generated'], c['existed'], c['erred'])
        yield result

    log.info("Generated: %6d  Existed:%6d  Erred: %6d  (FINISHED)",
             c['generated'], c['existed'], c['erred'])

def get_keys(obj):
    etag, bucket = obj.etag, obj.bucket
    s = IDigBioStorage()
    bucketbase = "idigbio-{0}-{1}".format(bucket, ENV)
    return CheckItem(etag, bucket,
                     s.get_key(etag, bucketbase),
                     s.get_key(etag + ".jpg", bucketbase + "-thumbnail"),
                     s.get_key(etag + ".jpg", bucketbase + "-fullsize"),
                     s.get_key(etag + ".jpg", bucketbase + "-webview"))


def check_and_generate(item):
    try:
        results = list(filter(None, generate_all(item)))
    except KeyboardInterrupt:
        raise
    except S3ResponseError:
        return None
    except Exception:
        log.exception("%s: Failed generating", item.etag)
        return None
    else:
        return GenerateResult(item.etag, results)


def generate_all(item):
    # check if thumbnail exists as proxy for everything existing
    if item.thumbnail.exists():
        log.debug("%s Thumbnail shortcut", item.etag)
        return
    img = get_media_img(item.media)
    yield build_deriv(item, img, 'fullsize')
    yield build_deriv(item, img, 'thumbnail')
    yield build_deriv(item, img, 'webview')


def build_deriv(item, img, deriv):
    key = getattr(item, deriv)
    if key.exists():
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


def upload_item(item):
    key = item.key
    data = item.data
    if isinstance(item, CopyItem):
        log.debug("%s copying from %s", key, data)
        data.copy(dst_bucket=key.bucket,
                  dst_key=key.name,
                  metadata={'Content-Type': 'image/jpeg'})
    else:
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
        log.error("%r failed downloading with %r %s", key, e, key)
        raise


def resize_image(img, deriv):
    derivative_width = WIDTHS[deriv]
    if img.size[0] > derivative_width:
        derivative_width_percent = (derivative_width / float(img.size[0]))
        derivative_horizontal_size = int(
            (float(img.size[1]) * float(derivative_width_percent)))
        derv = img.resize(
            (derivative_width, derivative_horizontal_size), Image.BILINEAR)
        return derv
    else:
        return img


def get_media_img(key):
    buff = key_to_buffer(key)
    if 'sounds' in key.bucket.name:
        log.debug("%s converting wave to img", key.name)
        return wave_to_img(buff)
    elif 'images' in key.bucket.name:
        img = Image.open(buff)
        if img.mode != 'RGB':
            img = img.convert('RGB')
        return img
    else:
        raise ValueError(
            "Unknown mediatype in bucket {0!r}, expected images or sounds".format(
                key.bucket.name))

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
