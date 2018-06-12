# This script relies on a lot of set operations (what is new, what's changed?) 
# and the best place to do that is in SQL rather than making data structures in
# Python memory. So, use a temp table to copy all the listing data from ceph
# into the DB and run queries there.

import os
import sys
import argparse

from idb.postgres_backend import apidbpool
from idb.helpers.logging import getLogger, configure_app_log
from idb.helpers.storage import IDigBioStorage

BUCKETS = ["idigbio-datasets-prod",
           "idigbio-images-prod",
           "idigbio-models-prod",
           "idigbio-sounds-prod",
           "idigbio-static-downloads",
           "idigbio-video-prod"]

TMP_TABLE = "ceph_objects_temp" # WARNING! Will be destroyed and re-created.


def build_temp_table(buckets):
    """Recreate the temporary table with Ceph files
    """

    # Drop and create temp table
    apidbpool.execute("DROP TABLE IF EXISTS {0};".format(TMP_TABLE))
    apidbpool.execute(("CREATE TABLE IF NOT EXISTS {0}( "
                   "ceph_bucket VARCHAR(32), "
                   "ceph_name VARCHAR(128), "
                   "ceph_bytes bigint, "
                   "ceph_etag uuid);").format(TMP_TABLE))


#    # Read through bucket inserting into temp table
#    logging.info("Importing bucket listing.")
#    inserted = 1
#    b = ceph.boto_conn.get_bucket(bucket)
#    for f in b.list():
#        db.cur.execute(("INSERT INTO {0} "
#                        "(etag, bytes, bucket, date) "
##                        "VALUES (?, ?, ?, ?)").format(tmp_table),
#                        (f.name, f.size, bucket, f.last_modified))
#        inserted += 1
#
#        if (inserted % 10000) == 0:
#            logging.info("Committing {0}".format(inserted))
#            db.con.commit()
#
#    db.con.commit()


def get_buckets(buckets):
    ceph 


if __name__ == '__main__':

    # this enables debug level logging
    configure_app_log(2, logfile="./update_db.log", journal='auto')

    argparser = argparse.ArgumentParser(
                description="Update the ceph_objects table with new and removed files that are stored in Ceph.")
    argparser.add_argument("--bucket", "-b",
                           required=False,
                           help="Just do one bucket instead of all configured buckets eg 'idigbio-images-prod'.")
    args = argparser.parse_args()
    if args.bucket != "":
        BUCKETS = [ args.bucket ]

    build_temp_table(BUCKETS)

    apidbpool.closeall()
