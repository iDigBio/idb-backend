from __future__ import division, absolute_import
from __future__ import print_function

import pytest

from idb.helpers import media_validation as mv


def test_validate_mime_for_type():
    assert (None, None) == mv.validate_mime_for_type(None, None)
    assert (None, 'images') == mv.validate_mime_for_type(None, 'images')
    assert ('image/jpeg', 'images') == mv.validate_mime_for_type('image/jpeg', None)
    assert ('image/jpeg', 'images') == mv.validate_mime_for_type('image/jpeg', 'images')
    assert ('audio/mpeg', 'sounds') == mv.validate_mime_for_type('audio/mpeg', None)
    assert ('audio/mpeg', 'sounds') == mv.validate_mime_for_type('audio/mpeg', 'sounds')
    assert ('audio/mpeg', 'sounds') == mv.validate_mime_for_type('audio/mpeg3', None)
    assert ('audio/mpeg', 'sounds') == mv.validate_mime_for_type('audio/mpeg3', 'sounds')
    assert ('application/zip', 'datasets') == mv.validate_mime_for_type('application/zip', 'datasets')
    assert ('model/mesh', 'models') == mv.validate_mime_for_type('model/mesh', None)
    assert ('model/mesh', 'models') == mv.validate_mime_for_type('model/mesh', 'models')

    with pytest.raises(mv.UnknownBucketError):
        mv.validate_mime_for_type('application/zip', None)
    with pytest.raises(mv.UnknownBucketError):
        mv.validate_mime_for_type('text/plain', None)
    with pytest.raises(mv.InvalidBucketError):
        mv.validate_mime_for_type('application/zip', 'foobar')
    with pytest.raises(mv.MimeNotAllowedError):
        mv.validate_mime_for_type('application/zip', 'images')


def test_validation_jpg(jpgpath):
    mime, mt = mv.validate(jpgpath.open('rb').read(1024))
    assert mime == u'image/jpeg'
    assert mt == u'images'

    mime, mt = mv.validate(jpgpath.open('rb').read(1024), mime=u'image/jpeg')
    assert mime == u'image/jpeg'
    assert mt == u'images'

    mime, mt = mv.validate(jpgpath.open('rb').read(1024), type='images')
    assert mime == u'image/jpeg'
    assert mt == u'images'

    mime, mt = mv.validate(jpgpath.open('rb').read(1024), mime=u'image/jpeg', type='images')
    assert mime == u'image/jpeg'
    assert mt == u'images'


def test_validation_png(pngpath):
    with pytest.raises(mv.UnknownBucketError):
        mime, mt = mv.validate(pngpath.open('rb').read(1024))

    with pytest.raises(mv.UnknownBucketError):
        mime, mt = mv.validate(pngpath.open('rb').read(1024), mime=u'image/png')

    with pytest.raises(mv.MimeNotAllowedError):
        mime, mt = mv.validate(pngpath.open('rb').read(1024), type='images')

    with pytest.raises(mv.MimeNotAllowedError):
        mime, mt = mv.validate(pngpath.open('rb').read(1024), mime=u'image/png', type='images')

def test_validation_bad_type(jpgpath):
    with pytest.raises(mv.InvalidBucketError):
        mv.validate(jpgpath.open('rb').read(1024), type="foo")
    with pytest.raises(mv.MimeNotAllowedError):
        mv.validate(jpgpath.open('rb').read(1024), type="sounds")
    with pytest.raises(mv.MimeNotAllowedError):
        mv.validate(jpgpath.open('rb').read(1024), type='images', mime='image/png')

def test_validation_zip(zippath):
    mime, mt = mv.validate(zippath.open('rb').read(1024), type="debugfile")
    assert mime == 'application/zip'
    assert mt == 'debugfile'

    mime, mt = mv.validate(zippath.open('rb').read(1024), type="datasets")
    assert mime == 'application/zip'
    assert mt == 'datasets'

    with pytest.raises(mv.UnknownBucketError):
        mime, mt = mv.validate(zippath.open('rb').read(1024))


def test_validation_mesh():
    mime, mt = mv.validate('', mime='model/mesh', url='foo.stl')
    assert mime == 'model/mesh'
    assert mt == 'models'
    mime, mt = mv.validate('', mime='model/mesh', url='foo.stl', type='models')
    assert mime == 'model/mesh'
    assert mt == 'models'


def test_mime_mismatch(jpgpath, zippath):
    with pytest.raises(mv.UnknownBucketError):
        mv.validate(jpgpath.open('rb').read(1024), mime='application/zip')

    with pytest.raises(mv.MimeMismatchError):
        mv.validate(zippath.open('rb').read(1024), mime='image/jpeg')

    with pytest.raises(mv.MimeMismatchError):
        mv.validate(jpgpath.open('rb').read(1024), type='datasets', mime='application/zip')

    with pytest.raises(mv.MimeMismatchError):
        mv.validate(zippath.open('rb').read(1024), mime='image/jpeg', type='images')
