import psycopg2
import uuid
import datetime
import random
import json
import hashlib
import statistics
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

TEST_SIZE=10000
TEST_COUNT=10

conn = psycopg2.connect(host="localhost",user="test",password="test",dbname="prod_copy")

def junk_ettager(d):
    h = hashlib.md5()
    h.update(json.dumps(d,sort_keys=True).encode("utf8"))
    return h.hexdigest()


def timeit(x):
    t = datetime.datetime.now()
    x()
    return (datetime.datetime.now() - t ).total_seconds()

def avg_time(x):
    times = []
    for _ in range(0,TEST_COUNT):
        times.append(timeit(x))

    if len(times) > 1:
        return (statistics.mean(times),statistics.median(times),statistics.stdev(times),min(times),times.index(min(times)),max(times),times.index(max(times)))
    else:
        return times

def row_count(cur,t):
    cur.execute("SELECT count(*) FROM " + t)
    return cur.fetchone()[0]

def get_metadata_view(cur,u=None):
    if u is None:
        cur.execute("SELECT * FROM idigbio_uuids WHERE deleted=false")
    else:
        cur.execute("SELECT * FROM idigbio_uuids WHERE deleted=false and uuids.id=%s", (u,))
    for r in cur:
        yield r

def get_data_view(cur,u=None):
    if u is None:
        cur.execute("SELECT * FROM idigbio_uuids_data WHERE deleted=false")
    else:
        cur.execute("SELECT * FROM idigbio_uuids_data WHERE deleted=false and uuids.id=%s", (u,))
    for r in cur:
        yield r

def get_metadata(cur,u=None):
    if u is None:
        cur.execute("""SELECT uuids.id as uuid,type,deleted,data_etag as etag,modified,version FROM uuids 
            LEFT JOIN LATERAL (
                SELECT * FROM uuids_data
                WHERE uuids_id=uuids.id
                ORDER BY modified DESC
                LIMIT 1
            ) AS latest
            ON uuids_id=uuids.id
            WHERE deleted=false
        """)
    else:
        cur.execute("""SELECT uuids.id as uuid,type,deleted,data_etag as etag,modified,version FROM uuids 
            LEFT JOIN LATERAL (
                SELECT * FROM uuids_data
                WHERE uuids_id=uuids.id
                ORDER BY modified DESC
                LIMIT 1
            ) AS latest
            ON uuids_id=uuids.id
            WHERE deleted=false and uuids.id=%s
        """, u)        
    for r in cur:
        yield r

def get_data(cur,u=None):
    if u is None:
        cur.execute("""SELECT uuids.id,type,deleted,etag,modified,version,data FROM uuids 
            LEFT JOIN LATERAL (
                SELECT * FROM uuids_data
                WHERE uuids_id=uuids.id
                ORDER BY modified DESC
                LIMIT 1
            ) AS latest
            ON uuids_id=uuids.id
            LEFT JOIN data
            ON data_etag = etag
            WHERE deleted=false
        """)
    else:
        cur.execute("""SELECT uuids.id,type,deleted,etag,modified,version,data FROM uuids 
            LEFT JOIN LATERAL (
                SELECT * FROM uuids_data
                WHERE uuids_id=uuids.id
                ORDER BY modified DESC
                LIMIT 1
            ) AS latest
            ON uuids_id=uuids.id
            LEFT JOIN data
            ON data_etag = etag
            WHERE deleted=false and uuids.id=%s
        """)        
    for r in cur:
        yield r

# UUID
def get_upsert_uuid():
    return """INSERT INTO uuids (id,type) 
        SELECT %(uuid)s as id, 'records' as type WHERE NOT EXISTS (
            SELECT 1 FROM uuids WHERE id=%(uuid)s
        )
    """

def upsert_uuid(cur,u):
    cur.execute(get_upsert_uuid(), {"uuid": u})
    conn.commit()

def upsert_uuid_l(cur,ul):
    cur.executemany(get_upsert_uuid(), [{"uuid": u} for u in ul])
    conn.commit()

# DATA
def get_upsert_data():
    return """INSERT INTO data (etag,data)
        SELECT %(etag)s as etag, %(data)s as data WHERE NOT EXISTS (
            SELECT 1 FROM data WHERE etag=%(etag)s
        )
    """

def upsert_data(cur,d):    
    cur.execute(get_upsert_data(), {"etag": junk_ettager(d), "data": json.dumps(d) })
    conn.commit()

def upsert_data_l(cur,dl):
    cur.executemany(get_upsert_data(), [{"etag": junk_ettager(d), "data": json.dumps(d) } for d in dl])
    conn.commit()

# UUID DATA
def get_upsert_uuid_data():
    return """WITH v AS (
        SELECT * FROM (
            SELECT data_etag, version, modified FROM uuids_data WHERE uuids_id=%(uuid)s 
            UNION 
            SELECT NULL as data_etag, 0 as version, NULL as modified
        ) as sq ORDER BY modified DESC NULLS LAST LIMIT 1
    )
    INSERT INTO uuids_data (uuids_id,data_etag,version)
        SELECT %(uuid)s as uuids_id, %(etag)s as data_etag, v.version+1 as version FROM v WHERE NOT EXISTS (
            SELECT 1 FROM uuids_data WHERE uuids_id=%(uuid)s AND data_etag=%(etag)s AND version=v.version
        )
    """

def upsert_uuid_data(cur,ud):
    cur.execute(get_upsert_uuid_data(), {"uuid": ud["uuid"], "etag": junk_ettager(ud["data"])})
    conn.commit()

def upsert_uuid_data_l(cur,udl):
    cur.executemany(get_upsert_uuid_data(), [{"uuid": ud["uuid"], "etag": junk_ettager(ud["data"])} for ud in udl])
    conn.commit()

def drop_schema(cur):
    cur.execute("""ALTER TABLE IF EXISTS idigbio_uuids RENAME to idigbio_uuids_bak""")

    cur.execute("DROP VIEW IF EXISTS idigbio_uuids_new")
    cur.execute("DROP VIEW IF EXISTS idigbio_uuids_data")
    cur.execute("DROP TABLE IF EXISTS uuids_data")    
    cur.execute("DROP TABLE IF EXISTS uuids")
    cur.execute("DROP TABLE IF EXISTS data")

def create_schema(cur):

    cur.execute("""CREATE TABLE IF NOT EXISTS uuids (
        id uuid NOT NULL PRIMARY KEY,
        type varchar(50) NOT NULL,
        parent uuid,
        deleted boolean NOT NULL DEFAULT false
    )""")


    cur.execute("""CREATE TABLE IF NOT EXISTS data (
        etag varchar(41) NOT NULL PRIMARY KEY,
        data jsonb
    )""")


    cur.execute("""CREATE TABLE IF NOT EXISTS uuids_data (
        id bigserial NOT NULL PRIMARY KEY,
        uuids_id uuid NOT NULL REFERENCES uuids(id),
        data_etag varchar(41) NOT NULL REFERENCES data(etag),
        modified timestamp NOT NULL DEFAULT now(),
        version int NOT NULL DEFAULT 1
    )""")

    cur.execute("CREATE INDEX uuids_data_uuids_id ON uuids_data (uuids_id)")
    cur.execute("CREATE INDEX uuids_data_modified ON uuids_data (modified)")
    cur.execute("CREATE INDEX uuids_deleted ON uuids (deleted)")
    cur.execute("CREATE INDEX uuids_parent ON uuids (parent)")
    cur.execute("CREATE INDEX uuids_type ON uuids (type)")

    cur.execute("""CREATE OR REPLACE VIEW idigbio_uuids_new AS 
        SELECT 
            uuids.id as id,
            type,
            deleted,
            data_etag as etag,
            version,
            modified,
            parent
        FROM uuids 
        LEFT JOIN LATERAL (
            SELECT * FROM uuids_data
            WHERE uuids_id=uuids.id
            ORDER BY modified DESC
            LIMIT 1
        ) AS latest
        ON uuids_id=uuids.id
    """)

    cur.execute("""CREATE OR REPLACE VIEW idigbio_uuids_data AS
        SELECT 
            uuids.id as id,
            type,
            deleted,
            data_etag as etag,
            version,
            modified,
            parent,
            data
        FROM uuids 
        LEFT JOIN LATERAL (
            SELECT * FROM uuids_data
            WHERE uuids_id=uuids.id
            ORDER BY modified DESC
            LIMIT 1
        ) AS latest
        ON uuids_id=uuids.id
        LEFT JOIN data
        ON data_etag = etag
    """)

def migrate_data():
    cur = conn.cursor()

    # Initial Run
    # print("migrate_ids",timeit(lambda: cur.execute("""INSERT INTO uuids (id,type,parent,deleted)
    #     SELECT id,type,parent,deleted FROM idigbio_uuids_bak
    # """)))

    # print("migrate_etags",timeit(lambda: cur.execute("""INSERT INTO data (etag)
    #     SELECT DISTINCT etag FROM idigbio_uuids_bak
    # """)))

    # print("migrate_versions",timeit(lambda: cur.execute("""INSERT INTO uuids_data (uuids_id,data_etag,version,modified)
    #     SELECT id,etag,version,modified FROM idigbio_uuids_bak
    # """)))

    print("migrate_ids",timeit(lambda: cur.execute("""WITH new_ids AS (
            SELECT id,type,parent,deleted FROM (
                SELECT id FROM idigbio_uuids_bak
                EXCEPT
                SELECT id FROM uuids
            ) as idlist NATURAL JOIN idigbio_uuids_bak             
        )
        INSERT INTO uuids (id,type,parent,deleted)
        SELECT * FROM new_ids
    """)))

    print("migrate_etags",timeit(lambda: cur.execute("""WITH new_etags AS (           
            SELECT etag as data FROM idigbio_uuids_bak
            EXCEPT
            SELECT etag as data FROM data
        )
        INSERT INTO data (etag)
        SELECT * FROM new_etags
    """)))

    print("migrate_versions",timeit(lambda: cur.execute("""WITH new_versions AS (
            SELECT idlist.id as uuids_id,idlist.etag as data_etag,idlist.version as version,idigbio_uuids_bak.modified as modified FROM (
                SELECT id,etag,version FROM idigbio_uuids_bak
                EXCEPT
                SELECT uuids_id, data_etag,version FROM uuids_data
            ) as idlist JOIN idigbio_uuids_bak ON idigbio_uuids_bak.id=idlist.id
        )
        INSERT INTO uuids_data (uuids_id,data_etag,version,modified)
        SELECT * FROM new_versions
    """)))

    conn.commit()    

def main():    
    cur = conn.cursor()
    #drop_schema(cur)
    #create_schema(cur)
    #conn.commit()

    migrate_data()

    # uuids = [str(uuid.uuid4()) for _ in range(0,TEST_SIZE)]
    # data = [{"randomID": x} for x in range(0,TEST_SIZE)]
    # uuids_data = [{"uuid": u, "data":d } for u,d in zip(uuids,data)]
    # uuids_data2 = [{"uuid": u, "data":d } for u,d in zip(uuids,data[1:] + [data[0]])]

    # print("uuids", avg_time(lambda: upsert_uuid_l(cur,uuids)))
    # print("data", avg_time(lambda: upsert_data_l(cur,data)))
    # print("uuids_data", avg_time(lambda: upsert_uuid_data_l(cur,uuids_data)))
    # print("uuids_data2", avg_time(lambda: upsert_uuid_data_l(cur,uuids_data2)))
    # print("uuids_data", avg_time(lambda: upsert_uuid_data_l(cur,uuids_data)))

    # conn.commit()
    print(row_count(cur, "uuids"))
    print(row_count(cur, "data"))
    print(row_count(cur, "uuids_data"))

    # print("mtime",avg_time(lambda: [r for r in get_metadata(cur)]))
    # print("vmtime",avg_time(lambda: [r for r in get_metadata_view(cur)]))
    # print("dtime",avg_time(lambda: [r for r in get_data(cur)]))
    # print("vdtime",avg_time(lambda: [r for r in get_data_view(cur)]))

    # conn.commit()

    # cur.close()
    # conn.close()

if __name__ == '__main__':
    main()