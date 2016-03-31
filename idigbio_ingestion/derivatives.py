from __future__ import division, absolute_import
from __future__ import print_function, unicode_literals
from future_builtins import map, filter

import traceback
import cStringIO
import sys


from collections import Counter, namedtuple
from gevent.pool import Pool
from gevent.monkey import patch_all
from PIL import Image

from idb.helpers.storage import IDigBioStorage
from idb.postgres_backend import apidbpool, NamedTupleCursor
from idigbio_ingestion.lib.log import getIDigBioLogger


WIDTHS = {
    'thumbnail': 260,
    'webview': 600
}

ENV = 'prod'

log = getIDigBioLogger('derivatives')


CheckItem = namedtuple(
    'CheckItem', ['etag', 'bucket', 'media', 'thumbnail', 'fullsize', 'webview'])

GenerateResult = namedtuple('GenerateResult', ['etag', 'items'])
GenerateItem = namedtuple('GenerateItem', ['key', 'data'])
CopyItem = namedtuple('CopyItem', ['key', 'data'])


def main(bucket):
    sql = ("SELECT etag, bucket FROM objects where derivatives=false and bucket=%s",
           (bucket,))
    objects = apidbpool.fetchall(sql, cursor_factory=NamedTupleCursor)
    pool = Pool(10)
    results = process_objects(objects, pool)

    def create_items(gr):
        for item in gr.items:
            upload_item(item)
        return gr.etag
    etags = pool.imap_unordered(create_items, results)

    apidbpool.executemany(
        "SELECT etag, bucket FROM objects where derivatives=false and bucket=%s",
        etags,
        autocommit=True
    )
    pool.join()


def process_objects(objects, pool):
    log.info("Checking derivatives for %d objects", objects)
    c = Counter()
    items = pool.imap_unordered(get_keys, objects, maxsize=100)
    items = pool.imap_unordered(check_and_generate, items, maxsize=100)

    for count, result in enumerate(items):
        if result is None:
            c['erred'] += 1
        else:
            if len(result.items):
                c['generated'] += 1
            else:
                c['existed'] += 1

            yield result.etag

        if count % 100 == 0:
            log.info("Generated: %10d  Existed:%10d  Erred: %10d",
                     c['generated'], c['existed'], c['erred'])

    log.info("Generated: %10d  Existed:%10d  Erred: %10d",
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
        return GenerateResult(item.etag, list(filter(None, generate_all(item))))
    except:
        log.exception("Failed generating: %s", item.etag)
        return None


def generate_all(item):
    # check if thumbnail exists as proxy for everything existing
    if item.thumbnail.exists():
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


def upload_item(item):
    key = item.key
    data = item.data
    if isinstance(item, CopyItem):
        data.copy(dst_bucket=key.bucket,
                  dst_key=key.name,
                  metadata={'Content-Type': 'image/jpeg'})
    else:
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
    except Exception:
        raise Exception(traceback.format_exc() +
                        "Exception during processing of " + key.name)


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
        log.debug("Converting wave to img %s", key.name)
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
    patch_all()
    if len(sys.argv) > 1:
        for bucket in sys.argv[1:]:
            main(bucket)
    else:
        print("""Usage:  derivatives.py <BUCKET ...>

    BUCKET can be one of {images, sounds}
        """, file=sys.stderr)
