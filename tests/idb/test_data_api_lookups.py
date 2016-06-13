import base64
import pytest
from flask import url_for


@pytest.fixture()
def testrsuuid():
    "This is the uuid of a small recordset that is part of our test schema"
    return "433d3c37-8dde-42e4-a344-2cb6605c5da2"


def test_v2_item(client, testrsuuid):
    r = client.get(url_for('idb.data_api.v2.item', t='recordsets', u=testrsuuid))
    assert r.status_code == 200
    assert r.json
    assert r.json['uuid'] == testrsuuid


def test_v2_subitem(client, testrsuuid):
    url = url_for('idb.data_api.v2.subitem', t='recordsets', u=testrsuuid, st='records')
    r = client.get(url)
    assert r.status_code == 200
    assert r.json
    assert r.json['itemCount'] == 214


def test_v2_item_no_type(client, testrsuuid):
    url = url_for('idb.data_api.v2.item_no_type', u=testrsuuid)
    r = client.get(url)
    assert r.status_code == 200
    assert r.json
    assert r.json['uuid'] == testrsuuid


def test_list(client, testrsuuid):
    url = url_for('idb.data_api.v2.list', t='recordsets')
    r = client.get(url)
    assert r.status_code == 200
    assert r.json
    assert r.json['itemCount'] == 2


def test_list_error(client):
    url = url_for('idb.data_api.v2.list', t='foobar')
    r = client.get(url)
    assert r.status_code == 404
