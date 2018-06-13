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

logger = getLogger("update-ceph-files")

def build_temp_table(buckets):
    """Recreate the temporary table with Ceph files
    """

    # Drop and create temp table
    apidbpool.execute("DROP TABLE IF EXISTS {0};".format(TMP_TABLE))
    apidbpool.execute(("CREATE TABLE IF NOT EXISTS {0}( "
                   "ceph_bucket VARCHAR(32), "
                   "ceph_name VARCHAR(128), "
                   "ceph_date TIMESTAMP WITHOUT TIME ZONE, "
                   "ceph_bytes bigint, "
                   "ceph_etag uuid);").format(TMP_TABLE))

    storage = IDigBioStorage()

    # Read through bucket inserting into temp table
    with apidbpool.connection(autocommit=False) as conn: # use a single connection from the pool to commit groups of statements
        cur = conn.cursor()
        inserted = 1
        for bucket in buckets:
            logger.info("Importing bucket listing for {0}.".format(bucket))
            b = storage.get_bucket(bucket)
            for f in b.list():
                # see backfill_new_etags() for why no etag here
                #key = b.get_key(f.name)
                cur.execute(("INSERT INTO {0} "
                               "(ceph_bucket, ceph_name, ceph_date, ceph_bytes) "
                               "VALUES (%s, %s, %s, %s)").format(TMP_TABLE),
                              (bucket, f.name, f.last_modified, f.size))
                inserted += 1

            if (inserted % 10000) == 0:
                logger.info("Committing {0}".format(inserted))
                conn.commit()

        conn.commit()


def get_buckets(buckets):
    ceph 


# The list() method on a bucket gets partial metadata so we have to HEAD each
# key individually to get the etags. HEADing each object is painfully slow, 
# 4 min vs 3 sec for 2k records, populate temp table w/o etags, just list bucket results. Then
# backfill the info in parallel later for only new things. This means we can't croscheck db and ceph for
# changing etags. Not sure under what circumstances that could happen and why we'd want to make sure it
# didn't. Is last_modified a good enough check?
def backfill_new_etags():
    pass


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
