import base64
import pytest
from flask import url_for

from idb.postgres_backend import db

#### Media stuff


@pytest.fixture()
def basic_auth_header(testdata):
    uuid = "872733a2-67a3-4c54-aa76-862735a5f334"
    key = "3846c98586668822ba6d5cb69caeb4c6"
    return ('Authorization', 'Basic ' + base64.b64encode("{}:{}".format(uuid, key)))


@pytest.fixture()
def testmedia_result():
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


def test_lookup_uuid(client, testmedia_result):
    tmr = testmedia_result
    url = url_for('idb.data_api.v2_media.lookup_uuid', u=tmr['uuid'], format="json")
    r = client.get(url)
    assert r.status_code == 200
    assert r.json
    for k in ('etag', 'mime', 'user', 'filereference', 'url', 'last_status', 'type'):
        assert r.json[k] == tmr[k]


def test_lookup_uuid_missing(client):
    url = url_for('idb.data_api.v2_media.lookup_uuid', u="asdfasdfasdfasdfasf", format="json")
    r = client.get(url)
    assert r.status_code == 404


def test_lookup_fileref(client, testmedia_result):
    tmr = testmedia_result
    url = url_for('idb.data_api.v2_media.lookup_ref', filereference=tmr['filereference'], format="json")
    r = client.get(url)
    assert r.status_code == 200
    assert r.json
    for k in ('etag', 'mime', 'user', 'filereference', 'url', 'last_status', 'type'):
        assert r.json[k] == tmr[k]


def test_lookup_etag(client, testmedia_result):
    tmr = testmedia_result
    url = url_for('idb.data_api.v2_media.lookup_etag', etag=tmr['etag'], format="json")
    r = client.get(url)
    assert r.status_code == 200
    assert r.json
    for k in ('etag', 'mime', 'user', 'filereference', 'url', 'last_status', 'type'):
        assert r.json[k] == tmr[k]


def test_redirect(client, testmedia_result):
    tmr = testmedia_result
    url = url_for('idb.data_api.v2_media.lookup_etag', etag=tmr['etag'])
    r = client.get(url)
    assert r.status_code == 302
    assert r.location == tmr['url']


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
    assert r.status_code == 400, r.json

    r = client.post(url, data={'mime': 'image/jpeg'}, headers=[basic_auth_header])
    assert r.status_code == 400, r.json

    r = client.post(url, data={'media_type': 'images'},
                    headers=[basic_auth_header])
    assert r.status_code == 400, r.json

    r = client.post(url, data={'media_type': 'images', 'mime': 'image/jpeg'},
                    headers=[basic_auth_header])
    assert r.status_code == 200, r.json
    assert r.json.get('last_status') is None


def test_upload_jpeg(client, testmedia_result, basic_auth_header, jpgpath, mock):
    mock.patch.object(db.MediaObject, 'upload', autospec=True)
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
    mock.patch.object(db.MediaObject, 'get_key', autospec=True)
    tmr = testmedia_result
    filereference = "http://test.idigbio.org/idigbio_logo.jpg"
    url = url_for('idb.data_api.v2_media.upload',
                  filereference=filereference, etag=tmr['etag'])
    r = client.post(url, headers=[basic_auth_header])
    assert r.status_code == 200
    assert r.json['filereference'] == filereference
    assert r.json['last_status'] == 200
    for k in ('etag', 'mime', 'user', 'url', 'last_status', 'type'):
        assert r.json[k] == tmr[k]
