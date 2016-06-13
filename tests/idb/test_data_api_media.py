import base64
import pytest
from flask import url_for

import boto.s3.key
from idb.postgres_backend.db import MediaObject


#### Media stuff


@pytest.fixture()
def basic_auth_header(testdata):
    uuid = "872733a2-67a3-4c54-aa76-862735a5f334"
    key = "3846c98586668822ba6d5cb69caeb4c6"
    return ('Authorization', 'Basic ' + base64.b64encode("{}:{}".format(uuid, key)))


@pytest.fixture()
def testmedia_result(testdata):
    ""
    return {
        'uuid': "00dd0642-e3e3-4f50-b258-9f3f000e3e6a",
        'etag': "3e17584bc43cf36617b6793515089656",
        'mime': "image/jpeg",
        'user': '872733a2-67a3-4c54-aa76-862735a5f334',
        'filereference': 'http://hasbrouck.asu.edu/imglib/pollen/Seeds/Cornus-stolonifera-272.jpg',
        'url': 'https://s.idigbio.org/idigbio-images-test/3e17584bc43cf36617b6793515089656',
        'modified': '2016-03-07 20:21:13.098036',
        'last_status': 200,
        'type': 'images'
    }

def check_response_props(tmr, r, status=200,
                         keys=('etag', 'mime', 'user', 'filereference', 'url', 'last_status', 'type')):
    __tracebackhide__ = True
    assert r.status_code == 200
    assert r.json
    for k in keys:
        assert r.json[k] == tmr[k], "Mismatch on {0!r}".format(k)


@pytest.fixture(autouse=True)
def block_upload_to_ceph(mock):
    "During the course of these tests we don't actually want to post to Ceph"
    mock.patch.object(MediaObject, 'upload', autospec=True)


def test_lookup_uuid(client, testmedia_result):
    tmr = testmedia_result
    url = url_for('idb.data_api.v2_media.lookup_uuid', u=tmr['uuid'], format="json")
    r = client.get(url)
    check_response_props(tmr, r)


def test_lookup_uuid_missing(client):
    url = url_for('idb.data_api.v2_media.lookup_uuid', u="asdfasdfasdfasdfasf", format="json")
    r = client.get(url)
    assert r.status_code == 404


def test_lookup_fileref(client, testmedia_result):
    tmr = testmedia_result
    url = url_for('idb.data_api.v2_media.lookup_ref', filereference=tmr['filereference'], format="json")
    r = client.get(url)
    check_response_props(tmr, r)


def test_lookup_etag(client, testmedia_result):
    tmr = testmedia_result
    url = url_for('idb.data_api.v2_media.lookup_etag', etag=tmr['etag'], format="json")
    r = client.get(url)
    check_response_props(tmr, r)


def test_redirect(client, testmedia_result):
    tmr = testmedia_result
    url = url_for('idb.data_api.v2_media.lookup_etag', etag=tmr['etag'])
    r = client.get(url)
    assert r.status_code == 302
    assert r.location == tmr['url']


def test_derivation(client, testmedia_result):
    tmr = testmedia_result
    url = url_for('idb.data_api.v2_media.lookup_etag', etag=tmr['etag'], deriv='webview')
    r = client.get(url)
    assert r.status_code == 302
    assert 'webview' in r.location


def test_bad_derivation(client, testmedia_result):
    """We kinda think this should return a 400; but every other error we
    still think should be 200 (because we're not sure how the client
    will act).

    """
    tmr = testmedia_result
    url = url_for('idb.data_api.v2_media.lookup_etag',
                  etag=tmr['etag'], deriv='foobar', format="json")
    r = client.get(url)
    assert r.status_code == 200  # TODO: 400?
    assert r.json['text'] == 'Media Error'


def test_upload_auth(client, basic_auth_header):
    url = url_for('idb.data_api.v2_media.upload')
    r = client.post(url)
    assert r.status_code == 401
    r = client.post(url, headers=[basic_auth_header])
    assert r.status_code == 400


def test_upload_no_body_existing_url(client, testmedia_result, basic_auth_header):
    tmr = testmedia_result
    url = url_for('idb.data_api.v2_media.upload', filereference=tmr['filereference'])
    r = client.post(url, headers=[basic_auth_header])
    assert r.status_code == 200, "Existing url, should clear last_status and proceed"
    assert r.json.get('last_status') is None

    r = client.post(url, data={'mime': 'image/jpeg'}, headers=[basic_auth_header])
    assert r.status_code == 200, r.json
    assert r.json.get('last_status') is None

    r = client.post(url, data={'media_type': 'images'},
                    headers=[basic_auth_header])
    assert r.status_code == 200, r.json
    assert r.json.get('last_status') is None

    r = client.post(url, data={'media_type': 'images', 'mime': 'image/jpeg'},
                    headers=[basic_auth_header])
    assert r.status_code == 200, r.json
    assert r.json.get('last_status') is None


def test_upload_no_body_new_url_1(client, basic_auth_header):
    filereference = "http://test.idigbio.org/asdfadsfadsfa.jpg"
    url = url_for('idb.data_api.v2_media.upload', filereference=filereference)

    r = client.post(url, data={'media_type': 'images'}, headers=[basic_auth_header])
    assert r.status_code == 400, "No mime, should be incomplete request {0!r}".format(r.json)


def test_upload_no_body_new_url_2(client, basic_auth_header):
    filereference = "http://test.idigbio.org/asdfadsfadsfa.jpg"
    url = url_for('idb.data_api.v2_media.upload', filereference=filereference)
    r = client.post(url, data={'media_type': 'images', 'mime': 'image/jpeg'},
                    headers=[basic_auth_header])
    assert r.status_code == 200, r.json
    assert r.json.get('last_status') is None


def test_upload_no_body_new_url_3(client, basic_auth_header):
    filereference = "http://test.idigbio.org/asdfadsfadsfa.jpg"
    url = url_for('idb.data_api.v2_media.upload', filereference=filereference)
    r = client.post(url, data={'mime': 'image/jpeg'}, headers=[basic_auth_header])
    assert r.status_code == 200, "New url with a non-ambiguous mime should work"


def test_upload_jpeg(client, basic_auth_header, jpgpath):
    filereference = "http://test.idigbio.org/idigbio_logo.jpg"
    url = url_for('idb.data_api.v2_media.upload', filereference=filereference)
    r = client.post(url,
                    data={'file': (jpgpath.open('rb'), 'file')},
                    headers=[basic_auth_header])
    assert r.status_code == 200
    assert r.json['filereference'] == filereference
    assert r.json['last_status'] == 200
    assert r.json['mime'] == 'image/jpeg'
    assert r.json['type'] == 'images'
    assert r.json['etag'] == '0fd72727eb6e181c5ef91a5140431530'


def test_upload_etag(client, testmedia_result, basic_auth_header, mock):
    mock.patch.object(boto.s3.key.Key, 'exists', return_value=True)
    tmr = testmedia_result
    filereference = "http://test.idigbio.org/idigbio_logo.jpg"
    url = url_for('idb.data_api.v2_media.upload',
                  filereference=filereference, etag=tmr['etag'])
    r = client.post(url, headers=[basic_auth_header])
    assert r.status_code == 200
    assert r.json['filereference'] == filereference
    assert r.json['last_status'] == 200
    check_response_props(tmr, r, keys=('etag', 'mime', 'user', 'url', 'last_status', 'type'))


def test_upload_missing_etag_in_db(client, basic_auth_header, mock):
    mock.patch.object(boto.s3.key.Key, 'exists', return_value=True)
    filereference = "http://test.idigbio.org/idigbio_logo.jpg"
    url = url_for('idb.data_api.v2_media.upload',
                  filereference=filereference, etag="foobar")
    r = client.post(url, headers=[basic_auth_header])
    assert r.status_code == 404

def test_upload_missing_etag_in_ceph(client, basic_auth_header, mock):
    mock.patch.object(boto.s3.key.Key, 'exists', return_value=False)
    filereference = "http://test.idigbio.org/idigbio_logo.jpg"
    url = url_for('idb.data_api.v2_media.upload',
                  filereference=filereference, etag="foobar")
    r = client.post(url, headers=[basic_auth_header])
    assert r.status_code == 404


def test_bad_media_type(client, basic_auth_header):
    filereference = "http://test.idigbio.org/idigbio_logo.jpg"
    url = url_for('idb.data_api.v2_media.upload',
                  filereference=filereference)
    r = client.post(url, data={'media_type': 'foobar', 'mime': 'image/jpeg'},
                    headers=[basic_auth_header])
    assert r.status_code == 400, r.json


def test_datasets_mime_type(client, basic_auth_header, zippath, mock):
    """We currently allow uploading anything to the datasets bucket
    because there isn't really reliable validation we can do
    """
    mock.patch.object(boto.s3.key.Key, 'exists', return_value=True)
    filereference = "http://test.idigbio.org/dataset.zip"
    url = url_for('idb.data_api.v2_media.upload', filereference=filereference)
    r = client.post(url,
                    data={
                        'file': (zippath.open('rb'), 'file'),
                        'media_type': 'datasets',
                    },
                    headers=[basic_auth_header])
    assert r.status_code == 200, r.json
    assert r.json['mime'] == 'application/zip', "Expected the detected_mime in answer"
    assert r.json['type'] == 'datasets'


def test_mime_validation1(client, basic_auth_header, jpgpath):
    "Verify that the posted mime matches the object's mime"
    filereference = "http://test.idigbio.org/dataset.zip"
    url = url_for('idb.data_api.v2_media.upload', filereference=filereference)
    r = client.post(url,
                    data={
                        'file': (jpgpath.open('rb'), 'file'),
                        'mime': 'image/jp2',
                    },
                    headers=[basic_auth_header])
    assert r.status_code == 400

    # Does it still verify even if we post to a bucket that isn't
    # normally being checked?
    r = client.post(url,
                    data={
                        'file': (jpgpath.open('rb'), 'file'),
                        'mime': 'image/jp2',
                        'media_type': 'datasets'
                    },
                    headers=[basic_auth_header])
    assert r.status_code == 400


def test_mime_validation2(client, basic_auth_header, pngpath):
    "Verify that image/png gets rejected"
    filereference = "http://test.idigbio.org/dataset.zip"
    url = url_for('idb.data_api.v2_media.upload', filereference=filereference)
    r = client.post(url,
                    data={
                        'file': (pngpath.open('rb'), 'file'),
                    },
                    headers=[basic_auth_header])
    assert r.status_code == 400


def test_mime_validation3(client, basic_auth_header, zippath):
    "A debug accepts zip files"
    filereference = "http://test.idigbio.org/dataset.zip"
    url = url_for('idb.data_api.v2_media.upload', filereference=filereference)
    r = client.post(url,
                    data={
                        'file': (zippath.open('rb'), 'file'),
                        'mime': 'application/zip',
                        'media_type': 'debugfile'
                    },
                    headers=[basic_auth_header])
    assert r.status_code == 200

def test_mime_validation4(client, basic_auth_header, zippath):
    "A debug accepts zip files"
    filereference = "http://test.idigbio.org/dataset.zip"
    url = url_for('idb.data_api.v2_media.upload', filereference=filereference)
    r = client.post(url,
                    data={
                        'file': (zippath.open('rb'), 'file'),
                        'mime': 'application/zip',
                        'media_type': 'images'
                    },
                    headers=[basic_auth_header])
    assert r.status_code == 400, r.json
