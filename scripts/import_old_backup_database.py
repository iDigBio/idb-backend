# Import the old sqlite database that tracked backups of ceph objects in
# to a new table in the prod database that will track integrity operations
# and backup status on important ceph buckets.

import os
import sys
from idb.helpers.logging import getLogger, configure_app_log
logger = getLogger("import")
from idb.postgres_backend.db import PostgresDB

# no utf-8 here
import csv

# ceph_* -> comes from list ceph bucket
# tsm_* -> from TSM backup process
# ver_* -> from retrive from ceph verification
# rest_* -> from test restores
create_query = """CREATE TABLE IF NOT EXISTS
ceph_objects (
 ceph_bucket varchar(32) NOT NULL,
 ceph_name varchar(128) NOT NULL,
 ceph_date timestamp without time zone,
 ceph_bytes INTEGER,
 ceph_etag uuid,
 tsm_eligible BOOLEAN,
 tsm_status VARCHAR(16),
 tsm_last_success timestamp without time zone,
 tsm_last_failure timestamp without time zone,
 tsm_bytes INTEGER,
 tsm_path VARCHAR(32),
 ver_status VARCHAR(16),
 ver_last_success timestamp without time zone,
 ver_last_failure timestamp without time zone,
 rest_status VARCHAR(16),
 rest_last_success timestamp without time zone,
 rest_last_failure timestamp without time zone
);
ALTER TABLE ceph_objects OWNER TO idigbio;
"""

def csv_iter(fn):
    with open(fn, 'rb') as f:
        csv_file = csv.DictReader(f)
        for d in csv_file:
            # reformat data
            d["tsm_status"] = "sent" if d["tsm_date"] else None
            d["ceph_bytes"] = d["ceph_bytes"].replace(',', '')
            d["sent_bytes"] = d["sent_bytes"].replace(',', '')
            d["tsm_bytes"] = d["tsm_bytes"].replace(',', '')

            # fixup empty strings with NULL
            d["sent_date"] = d["sent_date"] if d["sent_date"] else None
            d["ceph_date"] = d["ceph_date"] if d["ceph_date"] else None
            d["tsm_date"] = d["tsm_date"] if d["tsm_date"] else None
            d["ceph_bytes"] = d["ceph_bytes"] if d["ceph_bytes"] else None
            d["tsm_bytes"] = d["tsm_bytes"] if d["tsm_bytes"] else None

            yield d



if __name__ == '__main__':

    fn = sys.argv[1]

    with PostgresDB() as db:
        db.execute(create_query)
        db.commit()

        q = """INSERT INTO ceph_objects
               (ceph_bucket,
                ceph_name,
                ceph_date,
                ceph_bytes,
                tsm_eligible,
                tsm_status,
                tsm_last_success,
                tsm_bytes,
                tsm_path
               )
               VALUES
               (%(bucket)s,
                %(etag)s,
                %(ceph_date)s,
                %(ceph_bytes)s,
                TRUE,
                %(tsm_status)s,
                %(sent_date)s,
                %(sent_bytes)s,
                %(tsm_path)s
               )
               ON CONFLICT DO NOTHING
            """
        c = 0
        for l in csv_iter(fn):
            #print(l)
            try:
                db.execute(q, l)
            except:
                print(l)
                raise
            c += 1
            if (c % 100000) == 0:
                db.commit()
                c = 0
#                break

        db.commit()

