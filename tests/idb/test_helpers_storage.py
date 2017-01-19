from __future__ import division, absolute_import, print_function

import pytest
from boto.exception import BotoServerError, BotoClientError, S3DataError

from idb.helpers.etags import calcFileHash


@pytest.fixture()
def bucketname():
    "The bucket we're going to use for test uploads and downloads"
    return 'idigbio-test'

@pytest.fixture()
def store():
    from idb.helpers.storage import IDigBioStorage
    return IDigBioStorage()


@pytest.fixture()
def existingkey(store, bucketname):
    "Lets roll with a key that has been setup to exist"
    k = store.get_key('29a452290a0441fa5f413c36a8ca6398', bucketname)
    assert k.exists()
    return k


def test_download_md5_validation(store, existingkey, tmpdir):
    testfile = tmpdir / "roll"
    with pytest.raises(S3DataError):
        store.get_contents_to_filename(existingkey, str(testfile), md5="asdfasdf")
    assert not testfile.exists()
    store.get_contents_to_filename(existingkey, str(testfile), md5=existingkey.name)


#@pytest.mark.skip(reason="Actually writes to storage")
def test_file_upload_download(store, bucketname, tmpdir):
    k = store.upload(store.get_key('foobar', bucketname), __file__, content_type="x-foo/bar")
    localmd5 = calcFileHash(__file__)
    assert k.md5 == localmd5

    k2 = store.get_key('foobar', bucketname)
    assert k2.exists()

    localdownload = tmpdir / 'foobar'
    store.get_contents_to_filename(k2, str(localdownload), localmd5)
    assert localdownload.exists()
    assert localmd5 == calcFileHash(str(localdownload))
    assert k2.content_type == 'x-foo/bar'



#@pytest.mark.skip(reason="Actually writes to storage")
def test_largefile_upload(store, bucketname, tmpdir, monkeypatch):
    monkeypatch.setattr(store, 'MAX_CHUNK_SIZE', 10 * (1024 ** 2))
    keyname = 'largefile'
    testfile = tmpdir / "testfile"
    with testfile.open('ab') as f:
        f.truncate(64 * (1024 ** 2))
    md5 = calcFileHash(str(testfile))
    k = store.upload(store.get_key(keyname, bucketname), str(testfile))
    testfile.remove()
    store.get_contents_to_filename(k, str(testfile), md5=md5)
    k.delete()
