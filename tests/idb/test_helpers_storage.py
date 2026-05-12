# -*- coding: utf-8 -*-
from __future__ import division, absolute_import, print_function

import os
import io
import pytest
from botocore.exceptions import ClientError

from idb.helpers.etags import calcFileHash
from idb.helpers.storage import IDigBioStorage


# ----------------------------------------------------------------------
# Fixtures
# ----------------------------------------------------------------------
@pytest.fixture(scope="module")
def store():
    """Initialise the boto3-backed storage wrapper."""
    return IDigBioStorage()


@pytest.fixture()
def bucketname(store, request):
    """Create (and eventually purge) a temporary test bucket."""
    name = "idigbio-test"
    s3 = store._resource
    client = store._client

    # Ensure bucket exists
    try:
        client.create_bucket(Bucket=name)
    except ClientError as exc:                      # already exists, fine
        if exc.response["Error"]["Code"] not in ("BucketAlreadyOwnedByYou",
                                                 "BucketAlreadyExists"):
            raise

    def purge():
        bucket = s3.Bucket(name)
    
        # Avoid multi-delete (DeleteObjects) which can require Content-MD5 on some S3-compatible backends
        for obj in bucket.objects.all():
            obj.delete()
    
        bucket.delete()

    request.addfinalizer(purge)
    return name



@pytest.fixture()
def existingkey(store, bucketname, pngpath):
    """Upload a small fixture file and return its Object resource."""
    key_name = calcFileHash(str(pngpath))
    obj = store.get_key(key_name, bucketname)
    store.upload(obj, str(pngpath), public=False)

    # helper: confirm the object now exists
    try:
        obj.load()
    except ClientError:
        pytest.skip("failed to upload test key")
    return obj


# ----------------------------------------------------------------------
# Tests
# ----------------------------------------------------------------------
def test_download_md5_validation(store, existingkey, tmpdir):
    testfile = tmpdir.join("roll")

    # wrong MD5 should raise
    with pytest.raises(ValueError):
        store.get_contents_to_filename(existingkey, str(testfile), md5="baduuid")

    assert not testfile.check()

    # correct MD5 should succeed
    store.get_contents_to_filename(existingkey,
                                   str(testfile),
                                   md5=existingkey.e_tag.strip('"'))
    assert testfile.check()


def test_file_upload_download(store, bucketname, tmpdir):
    obj = store.get_key("foobar", bucketname)
    local_md5 = calcFileHash(__file__)

    # upload
    store.upload(obj,
                 __file__,
                 content_type="x-foo/bar",
                 public=False)

    # verify remote ETag (single-part == MD5)
    assert obj.e_tag.strip('"') == local_md5

    # download and compare bytes
    local_download = tmpdir.join("foobar")
    store.get_contents_to_filename(obj, str(local_download), local_md5)

    assert local_download.check()
    assert local_md5 == calcFileHash(str(local_download))
    assert obj.content_type == "x-foo/bar"


def test_largefile_upload(store, bucketname, tmpdir):
    keyname = "largefile"
    testfile = tmpdir.join("largefile.dat")

    # generate ~22 MiB file
    with io.open(str(testfile), "wb") as fh:
        fh.truncate(22 * 1024 ** 2 + 34923)

    md5_local = calcFileHash(str(testfile))

    obj = store.get_key(keyname, bucketname)
    store.upload(obj, str(testfile), public=False)      # multipart path

    # fresh download -> bytes match
    testfile.remove()
    store.get_contents_to_filename(obj, str(testfile), md5=md5_local)
    obj.delete()

    assert calcFileHash(str(testfile)) == md5_local
