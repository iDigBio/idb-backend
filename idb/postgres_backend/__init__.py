from __future__ import absolute_import
import os
import copy
import psycopg2
import psycopg2.extensions

import idb.config
from .gevent_helpers import GeventedConnPool

from psycopg2.extras import DictCursor, NamedTupleCursor
from psycopg2.extensions import cursor

from psycopg2.extensions import ISOLATION_LEVEL_READ_COMMITTED, ISOLATION_LEVEL_AUTOCOMMIT

psycopg2.extensions.register_type(psycopg2.extensions.UNICODE)
psycopg2.extensions.register_type(psycopg2.extensions.UNICODEARRAY)


DEFAULT_OPTS = {
    'cursor_factory': DictCursor
}


def get_pool(config=idb.config.config):
    pg_conf = copy.deepcopy(config[u"postgres"])
    prefix = pg_conf.pop("db_prefix", "idb_")
    suffix = pg_conf.pop("db_suffix", "_" + idb.config.ENV)
    pg_conf["database"] = prefix + "api" + suffix
    for k, v in DEFAULT_OPTS.items():
        pg_conf.setdefault(k, v)
    return GeventedConnPool(**pg_conf)


apidbpool = get_pool()
