# This script relies on a lot of set operations (what is new, what's changed?)
# and the best place to do that is in SQL rather than making data structures in
# Python memory. So, use a temp table to copy all the listing data from ceph
# into the DB and run queries there.
#
# What are the kinds of things we need to update?
#
# 1) Insert new records in to ceph_objects for everything in ceph not already
#    tracked.
# 2) Flag things that are in both the table and ceph where the metadata does
#    not match.

import gevent
from gevent import monkey, pool
monkey.patch_all()


import os
import sys
import argparse
import string

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
                   "ceph_etag uuid, "
                   "ceph_status VARCHAR(8));").format(TMP_TABLE))

    storage = IDigBioStorage()

    # Possible start characters of names, used for iterating through subsets of the bucket.
    # Took 10 hours to list a 24M bucket and then got socket timeout, try smaller chunks at a time.
    # DRAGON WARNING - file names that don't start with one of these letters (eg UTF8, punctuation) will not be tracked!
    prefixes = string.ascii_letters + string.digits

    # Read through bucket inserting into temp table
    with apidbpool.connection(autocommit=False) as conn: # use a single connection from the pool to commit groups of statements
        cur = conn.cursor()
        inserted = 1
        for bucket in buckets:
            logger.info("Importing bucket listing for {0}.".format(bucket))
            b = storage.get_bucket(bucket)
            for p in prefixes:
                for f in b.list(prefix=p):
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


def flag_new_records():
    """Mark records in the ceph_objects_temp table 'new' if they are not in ceph_objects
    """
    logger.info("Flagging new records")
    return apidbpool.execute("""UPDATE {0}
        SET ceph_status='new'
        WHERE
          ctid IN (
            SELECT t.ctid
            FROM ceph_objects_temp t LEFT JOIN ceph_objects o
            ON t.ceph_bucket=o.ceph_bucket AND t.ceph_name=o.ceph_name
            WHERE o.ceph_name IS NULL
          )
        """.format(TMP_TABLE))


def flag_changed_records():
    """Compare records that exist in both tables and flag things as 'changed' if they differ
    """
    logger.info("Flagging changed records")
    return apidbpool.execute("""UPDATE {0}
        SET ceph_status='changed'
        WHERE
          ctid IN (
            SELECT t.ctid
             FROM ceph_objects_temp t JOIN ceph_objects o
             ON t.ceph_bucket=o.ceph_bucket AND t.ceph_name=o.ceph_name
             WHERE t.ceph_date!=o.ceph_date OR t.ceph_bytes!=o.ceph_bytes
          )
        """.format(TMP_TABLE))


def copy_new_to_ceph_objects():
    """Copy 'new' records from ceph_objects_temp to ceph_objects
    """
    logger.info("Copying new ceph records to main table")
    return apidbpool.execute("""INSERT INTO ceph_objects
        (ceph_bucket, ceph_name, ceph_date, ceph_bytes, ceph_etag)
        SELECT ceph_bucket, ceph_name, ceph_date, ceph_bytes, ceph_etag
        FROM {0}
        WHERE ceph_status='new'
        """.format(TMP_TABLE))


# The list() method on a bucket gets partial metadata so we have to HEAD each
# key individually to get the etags. HEADing each object is painfully slow, 
# 4 min vs 3 sec for 2k records, populate temp table w/o etags, just list bucket results. Then
# backfill the info in parallel later for only new things. This means we can't croscheck db and ceph for
# changing etags. Not sure under what circumstances that could happen and why we'd want to make sure it
# didn't. Is last_modified a good enough check?
def backfill_flagged_etags():
    """Update ceph_objects_temp etags where records have a flag of any kind
    """
    logger.info("Backfilling etags on new/changed records")
    cols = ["ceph_bucket", "ceph_name"]
    results = apidbpool.fetchall("""SELECT {0}
        FROM {1}
        WHERE ceph_status IS NOT NULL
        """.format(','.join(cols), TMP_TABLE))

    row_objs = [] # Convert to something we can use with pool.imap_unordered
    for row in results:
        row_objs.append(dict(zip(cols, row)))

    p = pool.Pool(3)
    results = p.imap_unordered(backfill_flagged_worker, row_objs)
    return sum(results)


def backfill_flagged_worker(row):
    """Parallel worker for backfill_flagged_etags()
    """
    storage = IDigBioStorage()
    #print(row)
    b = storage.get_bucket(row["ceph_bucket"]) # Two phase here because validate=False in storage class
    row["etag"] = b.get_key(row["ceph_name"]).etag[1:-1]
    #print(row["etag"])
    return apidbpool.execute("""UPDATE {0}
       SET ceph_etag=%(etag)s
       WHERE ceph_bucket=%(ceph_bucket)s AND ceph_name=%(ceph_name)s
       """.format(TMP_TABLE), row)


if __name__ == '__main__':

    # this enables debug level logging
    configure_app_log(2, logfile="./update_db.log", journal='auto')

    argparser = argparse.ArgumentParser(
                description="Update the ceph_objects table with new and removed files that are stored in Ceph.")
    argparser.add_argument("--bucket", "-b",
                           required=False,
                           help="Just do one bucket instead of all configured buckets eg 'idigbio-images-prod'.")
    argparser.add_argument("--force", "-f",
                           required=False, default=False, action='store_true',
                           help="Force updating main table with new records even if changed records are found.")
    args = argparser.parse_args()
    if args.bucket != "":
        BUCKETS = [ args.bucket ]

    build_temp_table(BUCKETS)
    new = flag_new_records()
    changed = flag_changed_records()

    logger.info("Found {0} new records".format(new))
    if changed > 0:
        logger.error("Found {0} changed records, inspect {1} for rows with ceph_status='changed'".format(changed, TMP_TABLE))
        if not args.force:
            raw_input("Press any key to continue or Ctl-C to cancel ")

    etagged = backfill_flagged_etags()
    copy_new_to_ceph_objects()

    apidbpool.closeall()
