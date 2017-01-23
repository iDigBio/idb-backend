from datetime import datetime, timedelta

import uuid
import json

import pytest
from flask import url_for


from idb.data_api import v2_download
from idb.data_api.v2_download import DOWNLOADER_TASK_PREFIX
from idb.helpers.logging import idblogger


logger = idblogger.getChild('test.api.downloads')

@pytest.fixture()
def fakeredcli(request):
    import fakeredis
    fsr = fakeredis.FakeStrictRedis()
    v2_download.get_redis_conn = lambda: fsr
    request.addfinalizer(fsr.flushall)
    return fsr


class FakeAsyncResult(object):
    id = None
    status = "PENDING"
    result = None

    def __call__(self, id=None):
        logger.debug("Initialized AsyncResult with id %s", id)
        self.id = id or str(uuid.uuid4())
        return self

    def ready(self):
        return self.status in ('FAILURE', 'SUCCESS')

    def successful(self):
        return self.status == 'SUCCESS'

    def failed(self):
        return self.status == 'FAILURE'


@pytest.fixture()
def fakeresult(request, mocker):
    from idigbio_workers import downloader
    far = FakeAsyncResult()
    mocker.patch.object(downloader, 'AsyncResult', far)
    mocker.patch.object(downloader, 'delay', lambda *args, **kwargs: far())
    return far


def setupredis(client, tid=None, task_status=None, download_url=None, error=None, link=True, hash="foobar"):
    tid = tid or str(uuid.uuid4())
    redisdata = {
        'created': datetime.now(),
        'query': json.dumps({
            "core_type":
            "records",
            "rq": {},
            "form": "dwca-csv",
        }),
        'hash': hash,
    }
    if task_status:
        redisdata['task_status'] = task_status
    if download_url:
        redisdata['download_url'] = download_url
    if error:
        redisdata['error'] = error
    client.hmset(DOWNLOADER_TASK_PREFIX + tid, redisdata)
    client.expire(DOWNLOADER_TASK_PREFIX + tid, timedelta(hours=1))
    if link:
        client.set(DOWNLOADER_TASK_PREFIX + redisdata['hash'], tid)
        client.expire(DOWNLOADER_TASK_PREFIX + redisdata['hash'], timedelta(hours=1))
    return tid


def test_status_complete_task(client, fakeredcli):
    tid = setupredis(fakeredcli, task_status="SUCCESS", download_url="http://example.com")
    resp = client.get(url_for('idb.data_api.v2_download.status', tid=tid))
    assert resp.status_code == 200
    assert resp.json['download_url'] == "http://example.com"
    assert resp.json['complete'] is True
    assert resp.json['task_status'] == 'SUCCESS'
    assert 'error' not in resp.json


def test_status_pending_task(client, fakeredcli, fakeresult):
    tid = setupredis(fakeredcli)
    fakeresult.status = "PENDING"
    resp = client.get(url_for('idb.data_api.v2_download.status', tid=tid))
    assert resp.status_code == 200
    assert resp.json['task_status'] == "PENDING"
    assert resp.json['complete'] is False
    assert 'download_url' not in resp.json
    assert 'error' not in resp.json


def test_status_pending_now_success(client, fakeredcli, fakeresult):
    tid = setupredis(fakeredcli)
    fakeresult.status = "SUCCESS"
    fakeresult.result = "http://foo/bar"
    resp = client.get(url_for('idb.data_api.v2_download.status', tid=tid))
    assert resp.status_code == 200
    assert resp.json['download_url'] == "http://foo/bar"
    assert resp.json['complete'] is True
    assert resp.json['task_status'] == 'SUCCESS'
    assert 'error' not in resp.json


def test_status_pending_now_failure(client, fakeredcli, fakeresult):
    tid = setupredis(fakeredcli)
    fakeresult.status = 'FAILURE'
    fakeresult.result = ValueError("woot")
    resp = client.get(url_for('idb.data_api.v2_download.status', tid=tid))
    assert resp.status_code == 200
    assert resp.json['error'] == "woot"
    assert resp.json['complete'] is True
    assert resp.json['task_status'] == 'FAILURE'
    assert 'download_url' not in resp.json
    assert tid != fakeredcli.get(DOWNLOADER_TASK_PREFIX + resp.json['hash']), \
                  "Task wasn't unlinked from query hash in redis"


def test_status_pending_now_failure_already_unlinked(client, fakeredcli, fakeresult):
    tid = setupredis(fakeredcli, link=False, hash="foobar")
    fakeredcli.set(DOWNLOADER_TASK_PREFIX + "foobar", "woot")
    fakeresult.status = 'FAILURE'
    fakeresult.result = ValueError("woot")
    resp = client.get(url_for('idb.data_api.v2_download.status', tid=tid))
    assert resp.status_code == 200
    assert resp.json['error'] == "woot"
    assert resp.json['complete'] is True
    assert resp.json['task_status'] == 'FAILURE'
    assert 'download_url' not in resp.json
    assert "woot" == fakeredcli.get(DOWNLOADER_TASK_PREFIX + "foobar")



def test_initialization_no_params(client):
    resp = client.get(url_for('idb.data_api.v2_download.download'))
    assert resp.status_code == 400


def test_initialization(client, fakeredcli, fakeresult):
    resp = client.get(url_for('idb.data_api.v2_download.download', rq={"genus": "acer"}))
    assert resp.status_code == 302
    assert 'Location' in resp.headers
    tid = resp.headers['Location'].split('/')[-1]

    data = fakeredcli.hgetall(DOWNLOADER_TASK_PREFIX + tid)
    assert data
    assert len(data) != 0
    assert data['query']
    assert data['hash']
    assert data['created']


def test_initialization_repeated_request(client, fakeredcli, fakeresult):
    "Two requests with the same query should create the same download task"
    resp = client.get(url_for('idb.data_api.v2_download.download', rq={"genus": "acer"}))
    assert resp.status_code == 302
    assert 'Location' in resp.headers
    tid = resp.headers['Location'].split('/')[-1]

    data = fakeredcli.hgetall(DOWNLOADER_TASK_PREFIX + tid)
    assert data
    assert len(data) != 0
    assert data['query']
    assert data['hash']
    assert data['created']

    assert fakeredcli.get(DOWNLOADER_TASK_PREFIX + data['hash']) == tid

    resp = client.get(url_for('idb.data_api.v2_download.download', rq={"genus": "acer"}))
    assert resp.status_code == 302
    assert 'Location' in resp.headers
    tid2 = resp.headers['Location'].split('/')[-1]

    assert tid == tid2


def test_initialization_failed_task(client, fakeredcli, fakeresult):
    "If a download task a has failed, a new POST should retry"
    resp = client.get(url_for('idb.data_api.v2_download.download', rq={"genus": "acer"}))
    assert resp.status_code == 302
    assert 'Location' in resp.headers
    tid = resp.headers['Location'].split('/')[-1]

    data = fakeredcli.hgetall(DOWNLOADER_TASK_PREFIX + tid)
    assert data
    assert len(data) != 0
    assert data['query']
    assert data['hash']
    assert data['created']

    assert fakeredcli.get(DOWNLOADER_TASK_PREFIX + data['hash']) == tid
    fakeresult.status = "FAILURE"

    resp = client.get(url_for('idb.data_api.v2_download.download', rq={"genus": "acer"}))
    assert resp.status_code == 302
    assert 'Location' in resp.headers
    tid2 = resp.headers['Location'].split('/')[-1]

    assert tid != tid2
