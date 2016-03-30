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

    def __init__(self, name, bucket, exists):
        self.name = name
        self._exists = exists
        self.bucket = NamedThing(bucket)

    def exists(self):
        return self._exists


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
        FakeKey(etag + ".jpg", bucketbase + "-thumbnail", True),
        FakeKey(etag + ".jpg", bucketbase + "-fullsize", True),
        FakeKey(etag + ".jpg", bucketbase + "-webview", True))


@pytest.fixture()
def nonexisting_CheckItem(rand_etag):
    etag = rand_etag
    bucket = 'images'
    bucketbase = "idigbio-{0}-{1}".format(bucket, 'test')
    return derivatives.CheckItem(
        etag, bucket,
        FakeKey(etag, bucketbase, True),
        FakeKey(etag + ".jpg", bucketbase + "-thumbnail", False),
        FakeKey(etag + ".jpg", bucketbase + "-fullsize", False),
        FakeKey(etag + ".jpg", bucketbase + "-webview", True))


@pytest.fixture()
def img_etag():
    return 'bac1d6137adc8974f200173b6a4f05b8'

@pytest.fixture()
def sounds_etag():
    #url: u'http://arctos.database.museum/media/10292573?open'
    return '01a3aede049196e9bbf44e9196f49075'

@pytest.fixture()
def thumb_key(img_etag):
    from idb.helpers.storage import IDigBioStorage
    return IDigBioStorage().get_key(img_etag + '.jpg', 'idigbio-images-prod-thumbnail')

@pytest.fixture()
def sounds_key(sounds_etag):
    from idb.helpers.storage import IDigBioStorage
    return IDigBioStorage().get_key(sounds_etag, 'idigbio-sounds-prod')


@pytest.fixture()
def img(thumb_key, request):
    i = derivatives.get_media_img(thumb_key)
    if request.param:
        i.convert('RGB')
    return i


def test_get_keys(rand_etag):
    DbReturn = namedtuple('DbReturn', ['etag', 'bucket'])
    r = DbReturn(rand_etag, 'sounds')

    keys = derivatives.get_keys(r)
    assert keys


def test_img_fetch(img):
    assert img.format == 'JPEG'


def test_sounds_fetch(sounds_key):
    assert sounds_key.bucket.name in ('idigbio-sounds-prod', 'idigbio-sounds-beta')
    assert derivatives.get_media_img(sounds_key)


def test_resize(img, monkeypatch):
    assert derivatives.resize_image(img, 'thumbnail')

    monkeypatch.setitem(derivatives.WIDTHS, 'foo', 32)
    assert derivatives.resize_image(img, 'foo')


def test_check_and_gen_existed(existing_CheckItem):
    gr = derivatives.check_and_generate(existing_CheckItem)
    assert gr.etag is existing_CheckItem.etag
    assert len(gr.items) == 0


def test_check_and_gen_nonexist(nonexisting_CheckItem, img, monkeypatch):
    ci = nonexisting_CheckItem
    monkeypatch.setattr(derivatives, 'get_media_img', lambda x: img)
    gr = derivatives.check_and_generate(ci)
    assert gr.etag is ci.etag
    assert len(gr.items) == 2
