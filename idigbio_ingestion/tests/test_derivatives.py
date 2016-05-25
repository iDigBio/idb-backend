from __future__ import division, absolute_import
from __future__ import print_function, unicode_literals

import pytest
import uuid
from collections import namedtuple
from cStringIO import StringIO

from idigbio_ingestion import derivatives


def pytest_generate_tests(metafunc):
    if 'img' in metafunc.fixturenames:
        metafunc.parametrize("img", (True, False), indirect=True)


NamedThing = namedtuple('NamedThing', ['name'])


class FakeKey(object):
    _exists = True
    name = None
    md5 = None

    def __init__(self, name, bucket, exists, contents=None):
        self.name = name
        self._exists = exists
        self.bucket = NamedThing(bucket)
        self.contents = contents

    def exists(self):
        return self._exists

    def get_contents_to_file(self, buff):
        if self.contents:
            buff.write(self.contents)
        return buff


@pytest.fixture()
def rand_etag():
    return unicode(uuid.uuid4())


@pytest.fixture()
def existing_CheckItem(rand_etag):
    etag = rand_etag
    bucket = 'images'
    bucketbase = "idigbio-{0}-{1}".format(bucket, 'test')
    return derivatives.CheckItem(
        etag, bucket,
        FakeKey(etag, bucketbase, True),
        [FakeKey(etag + ".jpg", bucketbase + "-thumbnail", True),
         FakeKey(etag + ".jpg", bucketbase + "-fullsize", True),
         FakeKey(etag + ".jpg", bucketbase + "-webview", True)])


@pytest.fixture()
def nonexisting_CheckItem(rand_etag):
    etag = rand_etag
    bucket = 'images'
    bucketbase = "idigbio-{0}-{1}".format(bucket, 'test')
    return derivatives.CheckItem(
        etag, bucket,
        FakeKey(etag, bucketbase, True),
        [FakeKey(etag + ".jpg", bucketbase + "-thumbnail", False),
         FakeKey(etag + ".jpg", bucketbase + "-fullsize", False),
         FakeKey(etag + ".jpg", bucketbase + "-webview", True)])

@pytest.fixture()
def empty_CheckItem(rand_etag):
    etag = rand_etag
    bucket = 'images'
    bucketbase = "idigbio-{0}-{1}".format(bucket, 'test')
    return derivatives.CheckItem(
        etag, bucket, FakeKey(etag, bucketbase, True), [])


@pytest.fixture()
def img_etag():
    return 'bac1d6137adc8974f200173b6a4f05b8'

@pytest.fixture()
def sounds_etag():
    #url: u'http://arctos.database.museum/media/10292573?open'
    return '01a3aede049196e9bbf44e9196f49075'

@pytest.fixture()
def img_key(img_etag):
    from idb.helpers.storage import IDigBioStorage
    return IDigBioStorage().get_key(img_etag, 'idigbio-images-prod')

@pytest.fixture()
def thumb_key(img_etag):
    from idb.helpers.storage import IDigBioStorage
    return IDigBioStorage().get_key(img_etag + '.jpg', 'idigbio-images-prod-thumbnail')


@pytest.fixture()
def img_item(img_etag, img_key):
    return derivatives.CheckItem(img_etag, 'images', img_key, [])

@pytest.fixture()
def sounds_item(sounds_etag, sounds_key):
    return derivatives.CheckItem(sounds_etag, 'sounds', sounds_key, [])


@pytest.fixture()
def sounds_key(sounds_etag):
    from idb.helpers.storage import IDigBioStorage
    return IDigBioStorage().get_key(sounds_etag, 'idigbio-sounds-prod')


@pytest.fixture()
def img(img_item):
    buff = derivatives.fetch_media(img_item.media)
    img = derivatives.convert_media(img_item, buff)
    return img


def test_get_keys(rand_etag):
    DbReturn = namedtuple('DbReturn', ['etag', 'bucket'])
    r = DbReturn(rand_etag, 'sounds')
    keys = derivatives.get_keys(r)
    assert keys
    assert len(keys.keys)


def test_img_fetch(img):
    assert img.format == 'JPEG'


def test_sounds_fetch(sounds_item):
    assert sounds_item.media.bucket.name in 'idigbio-sounds-prod'
    buff = derivatives.fetch_media(sounds_item.media)
    snd = derivatives.convert_media(sounds_item, buff)
    assert snd.mode == 'RGB'


def test_bad_key_bucket(rand_etag):
    item = derivatives.CheckItem(rand_etag, 'foobar', None, [])
    with pytest.raises(derivatives.BadImageError):
        derivatives.convert_media(item, None)


def test_resize(img, monkeypatch):
    assert derivatives.resize_image(img, 'thumbnail')
    monkeypatch.setitem(derivatives.WIDTHS, 'foo', 32)
    assert derivatives.resize_image(img, 'foo')


def test_check_all_existed(existing_CheckItem):
    ci = derivatives.check_all(existing_CheckItem)
    assert len(ci.keys) == 0


def test_check_and_gen_nonexist(nonexisting_CheckItem):
    ci = nonexisting_CheckItem
    ci = derivatives.check_all(ci)
    assert len(ci.keys) == 2

def test_generate_all_empty(empty_CheckItem):
    gr = derivatives.generate_all(empty_CheckItem)
    assert gr.etag == empty_CheckItem.etag
    assert len(gr.items) == 0


def test_generate_all_nonempty(nonexisting_CheckItem, monkeypatch, img):
    ci = nonexisting_CheckItem
    monkeypatch.setattr(derivatives, 'fetch_media', lambda x: None)
    monkeypatch.setattr(derivatives, 'convert_media', lambda x, y: img)
    gr = derivatives.generate_all(ci)
    assert gr.etag is ci.etag
    assert len(gr.items) == len(ci.keys)


def test_zero_waveform():
    buff = StringIO(b'\xff\xfbT\xc4\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00Info\x00\x00\x00\x0f\x00\x00\x00\x02\x00\x00\x02@\x00\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\x00\x00\x009LAME3.98r\x01\xa5\x00\x00\x00\x00-\xfe\x00\x00\x14@$\x06\xb8\xc2\x00\x00@\x00\x00\x02@\xfa%]^\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\xff\xfbT\xc4\x00\x03\xc0\x00\x01\xa4\x00\x00\x00 \x00\x004\x80\x00\x00\x04LAME3.98.4UUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUU\xff\xfbT\xc4U\x83\xc0\x00\x01\xa4\x00\x00\x00 \x00\x004\x80\x00\x00\x04UUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUU')
    img = derivatives.wave_to_img(buff)
    assert img
    assert len(derivatives.img_to_buffer(img).getvalue())
