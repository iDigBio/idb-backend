from __future__ import absolute_import
from __future__ import print_function
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


GenerateItem = namedtuple(
    'GenerateItem', ['etag', 'bucket', 'media', 'thumbnail', 'fullsize', 'webview'])

GenerateResult = namedtuple('GenerateResult', ['etag', 'desc'])


def main(bucket):
    sql = ("SELECT etag, bucket FROM objects where derivatives=false and bucket=%s",
           (bucket,))
    objects = apidbpool.fetchall(sql, cursor_factory=NamedTupleCursor)
    process_objects(objects)


def process_objects(objects):
    log.info("Checking derivatives for %d objects", objects)
    pool = Pool(10)
    c = Counter()
    with apidbpool.connection() as conn:
        with conn.cursor() as cursor:
            items = pool.imap_unordered(get_keys, objects, maxsize=100)
            items = pool.imap_unordered(generate_all, items, maxsize=100)
            for count, result in enumerate(items):
                c[result.desc] += 1
                if result.desc != 'erred':
                    sql = ("UPDATE objects SET derivatives=true WHERE etag=%s",
                           (result.etag, ))
                    cursor.execute(*sql)
                if count % 100 == 0:
                    log.info("Generated: %10d  Existed:%10d  Erred: %10d",
                             c['generated'], c['existed'], c['erred'])
                    conn.commit()
            conn.commit()

    log.info("Generated: %10d  Existed:%10d  Erred: %10d",
             c['generated'], c['existed'], c['erred'])

def get_keys(obj):
    etag, bucket = obj.etag, obj.bucket
    s = IDigBioStorage()
    bucketbase = "idigbio-{0}-{1}".format(bucket, ENV)
    return GenerateItem(etag, bucket,
                        s.get_key(etag),
                        s.get_key(etag + ".jpg", bucketbase + "-thumbnail"),
                        s.get_key(etag + ".jpg", bucketbase + "-fullsize"),
                        s.get_key(etag + ".jpg", bucketbase + "-webview"))


def generate_all(item):
    try:
        # check if thumbnail exists as proxy for everything existing
        if item.thumbnail.exists():
            return GenerateResult(item.etag, 'existed')

        img = get_img(item)
        if not item.fullsize.exists():
            if item.bucket == 'images' and img.format == 'JPEG':
                copy_key_as_jpeg(item.media, item.fullsize)
            else:
                generate_deriv(item, img, 'fullsize')
        generate_deriv(item, img, 'thumbnail')
        generate_deriv(item, img, 'webview')
        return GenerateResult(item.etag, 'generated')
    except:
        return GenerateResult(item.etag, 'erred')
        log.exception("Failed generating: %s", item.etag)


def generate_deriv(item, img, deriv):
    key = getattr(item, deriv)
    if key.exists():
        return

    if deriv != 'fullsize':
        img = resize_image(img, deriv)
    buff = img_to_buffer(img, format='JPEG', quality=95)
    key.set_metadata('Content-Type', 'image/jpeg')
    key.set_contents_from_file(buff)
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


def get_img(item):
    buff = key_to_buffer(item.media)
    if item.bucket == 'sounds':
        return wave_to_img(buff)
    else:
        return Image.open(buff).convert('RGB')

def wave_to_img(buff):
    from idigbio_ingestion.lib.waveform import Waveform
    return Waveform(buff).generate_waveform_image()


def copy_key_as_jpeg(from_key, to_key):
    from_key.copy(dst_bucket=to_key.bucket,
                  dst_key=to_key.name,
                  metadata={'Content-Type': 'image/jpeg'})
    if to_key.exists():
        to_key.make_public()

if __name__ == '__main__':
    patch_all()
    if len(sys.argv) > 1:
        for bucket in sys.argv[1:]:
            main(bucket)
    else:
        print("Usage:  derivatives.py <bucket ...>")
