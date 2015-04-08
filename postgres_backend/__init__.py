import psycopg2
import os
from psycopg2.extras import DictCursor

from config import config

# pg_conf = config["postgres"]
# pg_conf["database"] = "config["postgres"]["db_prefix"] + 'data_cache'"
# del pg_conf["db_prefix"]

# pg = psycopg2.connect(**pg_conf)

pg = None
if os.environ["ENV"] == "test":
    pg = psycopg2.connect(host="localhost",user="test",password="test",dbname="test")
else:
    pg = psycopg2.connect(host="c18node16.acis.ufl.edu",user="idigbio",password="idigbiotest",dbname="idb-prod-new")