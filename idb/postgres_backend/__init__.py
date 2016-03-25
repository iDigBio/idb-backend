from __future__ import absolute_import
import os
import copy
import psycopg2
import psycopg2.extensions

from .gevent_helpers import GeventedConnPool

from psycopg2.extras import DictCursor, NamedTupleCursor
from psycopg2.extensions import cursor

from psycopg2.extensions import ISOLATION_LEVEL_READ_COMMITTED, ISOLATION_LEVEL_AUTOCOMMIT


from idb.config import config

psycopg2.extensions.register_type(psycopg2.extensions.UNICODE)
psycopg2.extensions.register_type(psycopg2.extensions.UNICODEARRAY)

pg_conf = copy.deepcopy(config["postgres"])

prefix = pg_conf.pop("db_prefix", "")
suffix = pg_conf.pop("db_suffix", "")

pg_conf["database"] = prefix + "api" + suffix
pg_conf["cursor_factory"] = DictCursor

if os.environ["ENV"] == "test":
    pg_conf["host"] = "localhost"
    pg_conf["user"] = "test"
    pg_conf["password"] = "test"
    pg_conf["dbname"] = "test"

apidbpool = GeventedConnPool(**pg_conf)
