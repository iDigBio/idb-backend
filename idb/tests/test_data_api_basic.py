import base64
import pytest
from flask import url_for

def test_app(client):
    assert client.get(url_for('index')).status_code == 200

def test_app_db(client):
    from idb.data_api.common import idbmodel
    r = idbmodel.fetchone("SELECT 1")
    assert r[0] == 1
