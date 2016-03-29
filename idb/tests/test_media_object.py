from __future__ import division, absolute_import
from __future__ import print_function, unicode_literals

import os
import pytest
from py.path import local

from idb.helpers.media_validation import UnknownMediaTypeError
from idb.postgres_backend.db import PostgresDB, MediaObject


prodonly = pytest.mark.skipif(os.environ['ENV'] != 'prod', reason='Requires production DB')



def test_mobj_fromobj_jpg(jpgpath):
    mobj = MediaObject.fromobj(jpgpath.open('rb'))
    assert mobj.mime == 'image/jpeg'
    assert mobj.mtype == 'images'
    assert mobj.etag == jpgpath.computehash()


def test_mobj_fromobj_mp3(mp3path):
    mobj = MediaObject.fromobj(mp3path.open('rb'))
    assert mobj.mime == 'audio/mpeg'
    assert mobj.etag == mp3path.computehash()


def test_mobj_bad_validation(pngpath):
    with pytest.raises(UnknownMediaTypeError):
        MediaObject.fromobj(pngpath.open('rb'))


def test_mobj_from_url_None(testidbmodel):
    mobj = MediaObject.fromurl('a not found url shuold not be found', idbmodel=testidbmodel)
    assert mobj is None


def test_mobj_from_etag_None(testidbmodel):
    mobj = MediaObject.frometag('a not found url shuold not be found', idbmodel=testidbmodel)
    assert mobj is None


@prodonly
def test_mobj_from_url_live(idbmodel):
    mobj = MediaObject.fromurl('http://hasbrouck.asu.edu/imglib/seinet/DES/DES00043/DES00043839_lg.jpg',
                               idbmodel=idbmodel)
    assert mobj
    assert mobj.mtype == 'images'
    assert mobj.mime == 'image/jpeg'
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
    assert mobj.mtype == 'images'
    assert mobj.mime == 'image/jpeg'
    assert mobj.etag == "341997942e8e8bc02c76917c660e674f"
