from __future__ import division, absolute_import
from __future__ import print_function, unicode_literals

import os
import pytest
from datetime import datetime

from idb.helpers.media_validation import MediaValidationError
from idb.postgres_backend.db import MediaObject



def test_mobj_fromobj_jpg(jpgpath):
    mobj = MediaObject.fromobj(jpgpath.open('rb'), url='foo.jpg')
    assert mobj.detected_mime == mobj.mime == 'image/jpeg'
    assert mobj.bucket == mobj.type == 'images'
    assert mobj.etag == jpgpath.computehash()


def test_mobj_fromobj_mp3(mp3path):
    mobj = MediaObject.fromobj(mp3path.open('rb'), url='foo.mp3')
    assert mobj.detected_mime == mobj.mime == 'audio/mpeg'
    assert mobj.etag == mp3path.computehash()


def test_given_type_validation(jpgpath):
    mobj = MediaObject.fromobj(jpgpath.open('rb'), url='foo.png', type='images')
    assert mobj
    assert mobj.type == mobj.bucket == 'images'

    with pytest.raises(MediaValidationError):
        mobj = MediaObject.fromobj(jpgpath.open('rb'), url='foo.png', type='datasets')

    with pytest.raises(MediaValidationError):
        mobj = MediaObject.fromobj(jpgpath.open('rb'), url='foo.png', type='foobar')


def test_mobj_bad_validation(pngpath):
    with pytest.raises(MediaValidationError):
        MediaObject.fromobj(pngpath.open('rb'), url='foo.png')

    with pytest.raises(MediaValidationError):
        MediaObject.fromobj(pngpath.open('rb'), url='foo.png', type="adsf")

    with pytest.raises(MediaValidationError):
        MediaObject.fromobj(pngpath.open('rb'), url='foo.png', type="images")


def test_mobj_from_url_None(testidbmodel):
    mobj = MediaObject.fromurl('a not found url should not be found',
                               idbmodel=testidbmodel)
    assert mobj is None


def test_mobj_from_etag_None(testidbmodel):
    mobj = MediaObject.frometag(
        "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx", idbmodel=testidbmodel)
    assert mobj is None


def test_mobj_fromurl(testidbmodel, testdata):
    url = 'http://hasbrouck.asu.edu/imglib/pollen/Seeds/Cornus-stolonifera-272.jpg'
    mobj = MediaObject.fromurl(url, idbmodel=testidbmodel)
    assert mobj
    assert mobj.url == url
    assert mobj.type == 'images' == mobj.bucket
    assert mobj.mime == 'image/jpeg' == mobj.detected_mime
    assert mobj.owner
    assert mobj.etag == "3e17584bc43cf36617b6793515089656"


def test_mobj_frometag(testidbmodel, testdata):
    mobj = MediaObject.frometag("3e17584bc43cf36617b6793515089656", idbmodel=testidbmodel)
    assert mobj
    assert mobj.type == 'images'
    assert mobj.mime == 'image/jpeg' == mobj.detected_mime
    assert mobj.etag == "3e17584bc43cf36617b6793515089656"
    assert mobj.url == 'http://hasbrouck.asu.edu/imglib/pollen/Seeds/Cornus-stolonifera-272.jpg'
    assert mobj.type == 'images' == mobj.bucket
    assert mobj.mime == 'image/jpeg' == mobj.detected_mime
    assert mobj.owner


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


def test_mobj_media_idigibio_patch(testidbmodel):
    "URLs that are media.idigbio.org contain etags and there is special handling for them"
    etag = "924709c6ebbd34030468185a323a437"
    url = "http://media.idigbio.org/lookup/images/" + etag
    mo = MediaObject(etag=etag, bucket="images", detected_mime='image/jpeg')
    mo.ensure_object(idbmodel=testidbmodel)

    mfu = MediaObject.fromurl(url, idbmodel=testidbmodel)
    assert mfu.etag == etag
    assert mfu.detected_mime == 'image/jpeg'
    assert mfu.url is None


def test_mobj_apimedia_idigibio_patch(testidbmodel):
    "URLs that are api.idigbio.org/v2/media contain etags and there is special handling for them"
    etag = "924709c6ebbd34030468185a323a437"
    url = "https://api.idigbio.org/v2/media/" + etag
    mo = MediaObject(etag=etag, bucket="images", detected_mime='image/jpeg')
    mo.ensure_object(idbmodel=testidbmodel)

    mfu = MediaObject.fromurl(url, idbmodel=testidbmodel)
    assert mfu.etag == etag
    assert mfu.detected_mime == 'image/jpeg'
    assert mfu.url is None
