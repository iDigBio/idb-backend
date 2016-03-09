from __future__ import absolute_import
import requests
import traceback

from flask import jsonify
from werkzeug.exceptions import default_exceptions


s = requests.Session()


def json_error(status_code, message=None):
    if message is None:
        if status_code in default_exceptions:
            message = default_exceptions[status_code].description
    resp = jsonify({"error": message})
    resp.status_code = status_code
    return resp
