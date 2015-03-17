import psycopg2
from psycopg2.extras import DictCursor

from config import config

# pg_conf = config["postgres"]
# pg_conf["database"] = "config["postgres"]["db_prefix"] + 'data_cache'"
# del pg_conf["db_prefix"]

# pg = psycopg2.connect(**pg_conf)

pg = psycopg2.connect(host="localhost",user="test",password="test",dbname="prod_copy")