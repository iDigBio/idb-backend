import requests
import traceback

from flask import jsonify
from werkzeug.exceptions import default_exceptions

from .config import RIAK_URL

s = requests.Session()

def load_data_from_riak(t, u, e):
    if RIAK_URL is not None:
        try:
            print RIAK_URL.format(t,u,e)
            resp = s.get(RIAK_URL.format(t,u,e))
            resp.raise_for_status()
            return resp.json()["idigbio:data"]
        except:
            traceback.print_exc()
            return None
    else:
        return None

def json_error(status_code,message=None):
    if message is None:
        if status_code in default_exceptions:
            message = default_exceptions[status_code].description
    resp = jsonify({"error": message})
    resp.status_code = status_code
    return resp