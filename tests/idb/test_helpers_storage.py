from __future__ import division, absolute_import, print_function

import pytest
from boto.exception import BotoServerError, BotoClientError, S3DataError

from idb.helpers.etags import calcFileHash

@pytest.fixture()
def store(request):
    from idb.helpers.storage import IDigBioStorage
    store = IDigBioStorage()
    return store


@pytest.fixture()
def bucketname(store, request):
    "The bucket we're going to use for test uploads and downloads"
    name = 'idigbio-test'
    conn = store.boto_conn
    def purge():
        b = conn.get_bucket(name)
        for k in b:
            k.delete()
        conn.delete_bucket(b)
    request.addfinalizer(purge)
    store.boto_conn.create_bucket(name)
    return name


@pytest.fixture()
def existingkey(store, bucketname, pngpath):
    kn = calcFileHash(str(pngpath))
    k = store.get_key(kn, bucketname)
    k.set_contents_from_filename(str(pngpath))
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
    k = store.upload(store.get_key('foobar', bucketname), __file__, content_type="x-foo/bar", public=False)
    localmd5 = calcFileHash(__file__)
    assert k.md5.decode('utf-8') == localmd5

    k2 = store.get_key('foobar', bucketname)
    assert k2.exists()

    localdownload = tmpdir / 'foobar'
    store.get_contents_to_filename(k2, str(localdownload), localmd5)
    assert localdownload.exists()
    assert localmd5 == calcFileHash(str(localdownload))
    assert k2.content_type == 'x-foo/bar'



#@pytest.mark.skip(reason="Actually writes to storage")
def test_largefile_upload(store, bucketname, tmpdir, monkeypatch):
    monkeypatch.setattr(store, 'MAX_CHUNK_SIZE', 16 * (1024 ** 2))
    keyname = 'largefile'
    testfile = tmpdir / "testfile"
    with testfile.open('ab') as f:
        f.truncate(22 * (1024 ** 2) + 34923)
    md5 = calcFileHash(str(testfile))
    k = store.upload(store.get_key(keyname, bucketname), str(testfile), public=False)
    testfile.remove()
    store.get_contents_to_filename(k, str(testfile), md5=md5)
    k.delete()
    assert calcFileHash(str(testfile)) == md5
