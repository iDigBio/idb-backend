from __future__ import absolute_import
import requests
import traceback

from flask import jsonify, current_app
from werkzeug.exceptions import default_exceptions

# Find the stack on which we want to store the database connection.
# Starting with Flask 0.9, the _app_ctx_stack is the correct one,
# before that we need to use the _request_ctx_stack.
try:
    from flask import _app_ctx_stack as stack
except ImportError:
    from flask import _request_ctx_stack as stack


from idb.postgres_backend.db import PostgresDB


s = requests.Session()


def json_error(status_code, message=None):
    if message is None:
        if status_code in default_exceptions:
            message = default_exceptions[status_code].description
    resp = jsonify({"error": message})
    resp.status_code = status_code
    return resp


def get_idb():
    return PostgresDB(pool=current_app.config['DB'])

class IDBModelSession(object):
    app = None

    def __init__(self, app=None):
        self.app = app
        if app is not None:
            self.init_app(app)

    def init_app(self, app):
        app.config.setdefault('DB', apidbpool)
        # Use the newstyle teardown_appcontext if it's available,
        # otherwise fall back to the request context
        if hasattr(app, 'teardown_appcontext'):
            app.teardown_appcontext(self._teardown)
        else:
            app.teardown_request(self._teardown)

    def _teardown(self, exception):
        ctx = stack.top
        try:
            ctx.IDBModel.close()
        except AttributeError:
            pass

    def __getattr__(self, k):
        ctx = stack.top
        idb = None
        try:
            idb = ctx.IDBModel
        except:
            idb = PostgresDB(pool=current_app.config['DB'])
            ctx.IDBModel = idb
        return getattr(idb, k)

idbmodel = IDBModelSession()
