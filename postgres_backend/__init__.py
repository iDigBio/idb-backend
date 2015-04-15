import psycopg2
import os
import copy
from psycopg2.extras import DictCursor

from config import config

prefix = config["postgres"]["db_prefix"] if "db_prefix" in config["postgres"] else ""
suffix = config["postgres"]["db_suffix"] if "db_suffix" in config["postgres"] else ""

pg_conf = copy.deepcopy(config["postgres"])
del pg_conf["db_prefix"]
del pg_conf["db_suffix"]

pg_conf["database"] = prefix + 'api' + suffix

pg = None
if os.environ["ENV"] == "test":
    pg = psycopg2.connect(host="localhost",user="test",password="test",dbname="test")
else:
    pg = psycopg2.connect(**pg_conf)