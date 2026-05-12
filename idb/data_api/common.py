from __future__ import division, absolute_import, print_function
import requests

from flask import jsonify, current_app, g
from werkzeug.exceptions import default_exceptions

from idb.postgres_backend.db import PostgresDB
from idb.helpers.logging import idblogger


logger = idblogger.getChild('api')
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
        logger.debug("Initializing idbmodel connection with %r", app)
        # Use teardown_appcontext which is the modern approach
        app.teardown_appcontext(self._teardown)

    def _teardown(self, exception):
        # Use Flask's g object instead of stack
        if hasattr(g, 'IDBModel'):
            g.IDBModel.close()

    def __getattr__(self, k):
        # Use Flask's g object which is the application context local storage
        idb = getattr(g, 'IDBModel', None)
        if idb is None:
            idb = get_idb()
            g.IDBModel = idb
        return getattr(idb, k)


idbmodel = IDBModelSession()