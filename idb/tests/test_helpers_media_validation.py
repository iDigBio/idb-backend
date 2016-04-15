from __future__ import division, absolute_import
from __future__ import print_function

import pytest

from idb.helpers import media_validation as medval


def test_sniff_validation_jpg(jpgpath):
    mime, mtype = medval.sniff_validation(jpgpath.open('rb').read(1024))
    assert mime == u'image/jpeg'
    assert mtype == u'images'


def test_sniff_validation_png(pngpath):
    with pytest.raises(medval.UnknownMediaTypeError):
        mime, mtype = medval.sniff_validation(pngpath.open('rb').read(1024))


def test_sniff_validation_mp3(mp3path):
    mime, mtype = medval.sniff_validation(mp3path.open('rb').read(1024))
    assert mime == u'audio/mpeg'
    assert mtype == u'sounds'
