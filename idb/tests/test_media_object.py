from __future__ import division, absolute_import
from __future__ import print_function, unicode_literals

import os
import pytest
from datetime import datetime

from idb.helpers.media_validation import UnknownMediaTypeError
from idb.postgres_backend.db import MediaObject


prodonly = pytest.mark.skipif(os.environ['ENV'] != 'prod', reason='Requires production DB')


def test_mobj_fromobj_jpg(jpgpath):
    mobj = MediaObject.fromobj(jpgpath.open('rb'))
    assert mobj.detected_mime == mobj.mime == 'image/jpeg'
    assert mobj.bucket == mobj.type == 'images'
    assert mobj.etag == jpgpath.computehash()


def test_mobj_fromobj_mp3(mp3path):
    mobj = MediaObject.fromobj(mp3path.open('rb'))
    assert mobj.detected_mime == mobj.mime == 'audio/mpeg'
    assert mobj.etag == mp3path.computehash()


def test_given_type_validation(jpgpath):
    mobj = MediaObject.fromobj(jpgpath.open('rb'), type='images')
    assert mobj
    assert mobj.type == mobj.bucket == 'images'

    mobj = MediaObject.fromobj(jpgpath.open('rb'), type='datasets')
    assert mobj
    assert mobj.type == mobj.bucket == 'datasets'

    mobj = MediaObject.fromobj(jpgpath.open('rb'), type='foobar')
    assert mobj
    assert mobj.type == mobj.bucket == 'images'


def test_mobj_bad_validation(pngpath):
    with pytest.raises(UnknownMediaTypeError):
        MediaObject.fromobj(pngpath.open('rb'))

    with pytest.raises(UnknownMediaTypeError):
        MediaObject.fromobj(pngpath.open('rb'), type="adsf")


def test_mobj_from_url_None(testidbmodel):
    mobj = MediaObject.fromurl('a not found url should not be found',
                               idbmodel=testidbmodel)
    assert mobj is None


def test_mobj_from_etag_None(testidbmodel):
    mobj = MediaObject.frometag(
        "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx", idbmodel=testidbmodel)
    assert mobj is None


def test_mobj_full_run(testidbmodel, jpgpath):
    url = "http://test.idigbio.org/idigbio_logo.jpg"
    mobj = MediaObject.fromobj(jpgpath.open('rb'), url=url)
    assert mobj.url == "http://test.idigbio.org/idigbio_logo.jpg"
    assert mobj.etag
    etag = mobj.etag
    mobj.derivatives = False
    mobj.insert_object(testidbmodel)
    mobj.last_status = 200
    mobj.last_check = datetime.now()
    mobj.modified = datetime.now()
    mobj.ensure_media(testidbmodel)
    mobj.ensure_media_object(testidbmodel)

    mfe = MediaObject.frometag(etag, idbmodel=testidbmodel)
    assert mfe
    for s in mobj.__slots__:
        if s != 'modified':
            assert(getattr(mobj, s) == getattr(mfe, s))

    mfu = MediaObject.fromurl(url, idbmodel=testidbmodel)
    assert mfu
    assert mfu.url == url
    for s in mobj.__slots__:
        if s != 'modified':
            assert(getattr(mobj, s) == getattr(mfu, s))


@prodonly
def test_mobj_from_url_live(idbmodel):
    url = 'http://hasbrouck.asu.edu/imglib/seinet/DES/DES00043/DES00043839_lg.jpg'
    mobj = MediaObject.fromurl(url, idbmodel=idbmodel)
    assert mobj
    assert mobj.type == 'images' == mobj.bucket
    assert mobj.mime == 'image/jpeg' == mobj.detected_mime
    assert mobj.owner
    assert mobj.etag == "341997942e8e8bc02c76917c660e674f"
    from idb.helpers.storage import IDigBioStorage
    k = mobj.get_key(IDigBioStorage())
    assert k
    assert k.exists()


@prodonly
def test_mobj_from_etag_live(idbmodel):
    mobj = MediaObject.frometag('341997942e8e8bc02c76917c660e674f', idbmodel)
    assert mobj
    assert mobj.type == 'images'
    assert mobj.mime == 'image/jpeg' == mobj.detected_mime
    assert mobj.etag == "341997942e8e8bc02c76917c660e674f"
