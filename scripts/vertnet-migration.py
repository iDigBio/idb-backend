import pandas as pd
from psycopg2 import DatabaseError
from psycopg2.extras import DictCursor

from idb.postgres_backend.db import PostgresDB, RecordSet

db = PostgresDB()

df = pd.read_csv('vtnet-migrated.csv')

kvs = df.loc[df["uuid_y"].notnull(), ["id_y", "uuid_y"]].set_index("id_y")["uuid_y"].to_dict()

db.execute("BEGIN")

for k,v in kvs.items():
    rs_sql = "update recordsets set uuid = '{0}', ingest = true where id = '{1}'".format(v,k)
    print(rs_sql)
    db.execute(rs_sql)

db.commit()