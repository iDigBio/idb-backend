import psycopg2
import os
import copy

from psycopg2.extras import DictCursor

from idb.config import config
from .gevent_helpers import GeventedConnPool


pg_conf = copy.deepcopy(config["postgres"])
if 'db_prefix' in pg_conf: del pg_conf["db_prefix"]
if "db_suffix" in pg_conf: del pg_conf["db_suffix"]

pg_conf["dbname"] = "stats"
pg_conf["cursor_factory"] = DictCursor

if os.environ["ENV"] == "test":
    pg_conf["host"] = "localhost"
    pg_conf["user"] = "test"
    pg_conf["password"] = "test"

statsdbpool = GeventedConnPool(**pg_conf)
