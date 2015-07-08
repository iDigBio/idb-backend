from functools import wraps
from flask import request, Response, jsonify, current_app
import os
import traceback

from .encryption import _encrypt
from idb.config import config
from idb.postgres_backend.db import PostgresDB

db = PostgresDB()
cur = db.cursor()

def check_auth(username, password):
    """This function is called to check if a username /
    password combination is valid.
    """    
    try:
        corrections = "/v2/corrections" in request.url
        annotations = "/v2/anotations" in request.url
        objects = "/v2/media" in request.url
        if corrections:
            cur.execute("SELECT * FROM idb_api_keys WHERE user_uuid=%s and apikey=%s and corrections_allowed=true",(username, _encrypt(password,os.environ["IDB_CRYPT_KEY"])))
        elif annotations:
            cur.execute("SELECT * FROM idb_api_keys WHERE user_uuid=%s and apikey=%s and annotations_allowed=true",(username, _encrypt(password,os.environ["IDB_CRYPT_KEY"])))
        elif objects:
            cur.execute("SELECT * FROM idb_api_keys WHERE user_uuid=%s and apikey=%s and objects_allowed=true",(username, _encrypt(password,os.environ["IDB_CRYPT_KEY"])))
        else:
            cur.execute("SELECT * FROM idb_api_keys WHERE user_uuid=%s and apikey=%s and records_allowed=true",(username, _encrypt(password,os.environ["IDB_CRYPT_KEY"])))            
        r = cur.fetchone()
        if r is not None:
            return True
        else:
            return False
    except:
        db.rollback()
        return False

def authenticate():
    """Sends a 401 response that enables basic auth"""
    resp = jsonify({"error": "Requires Authentication"})
    resp.status_code = 401
    resp.headers = {'WWW-Authenticate': 'Basic realm="Login Required"'}
    return resp

def requires_auth(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        # Disable auth during internal testing
        test_mode = "TESTING" in current_app.config and current_app.config["TESTING"]
        auth = request.authorization
        if not test_mode and (not auth or not check_auth(auth.username, auth.password)):
            return authenticate()
        return f(*args, **kwargs)
    return decorated