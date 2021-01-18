from __future__ import division, absolute_import, print_function

import base64
import pytest
from flask import url_for
import idb

def test_app(client):
    assert client.get(url_for('index')).status_code == 200

def test_app_db(client):
    from idb.data_api.common import idbmodel
    r = idbmodel.fetchone("SELECT 1")
    assert r[0] == 1

@pytest.mark.readonly
def test_version(client):
    r = client.get(url_for('version'))
    assert r.status_code == 200
    assert str(r.data, "utf-8") == idb.__version__


@pytest.mark.readonly
def test_healthz(client, testdbpool):
    r = client.get(url_for('healthz'))
    assert r.status_code == 200
