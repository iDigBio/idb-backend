# Verify objects in ceph by downloading and checksumming them and making
# sure whats in the database matches what ceph is serving.
# Track status and state with the ceph_objects table in the databse.
import os
import sys
import argparse

from idb.postgres_backend import apidbpool
from idb.helpers.logging import getLogger, configure_app_log
from idb.helpers.storage import IDigBioStorage

TMP_DIR = os.path.join("/tmp", os.path.basename(sys.argv[0]))

logger = getLogger("verify-ceph-files")


def get_stat_object(bucket, name):
    """Get a stats object from Ceph for the requested object.
    """
    storage = IDigBioStorage()
    stat_obj = storage.get_key(name, bucket)
    print(stat_obj.md5)
    # HERE!
    return stat_obj

def get_row_objs_from_db(args):
    """Get a list of objects to verify from the database based on user's
    passed arguments.
    """
    cols = ["ceph_bucket", "ceph_name", "ceph_date", "ceph_bytes",
            "ver_status", "ver_last_success", "ver_last_failure"]
    rows = apidbpool.fetchall("""SELECT {} FROM ceph_objects WHERE
                                  length(ceph_name) > 10 AND
                                  ver_status IS NULL
                                  LIMIT %(count)s""".format(','.join(cols)),
                                  vars(args)) # vars() namespace to dict conversion
    row_objs = []
    for row in rows:
        row_objs.append(dict(zip(cols, row)))
    return row_objs

def verify_object(row_obj, stat_obj):
    """Download an object and check if against the expected metadata."""
    return True

def update_db_metadata(row_obj, stat_obj):
    """Some db records are incomplete due to the original db being used
    for backups and not a full accounting of object properties, backfill
    any missing information into the database.
    """
    return True

def verify_all_objects_worker(row_obj):
    """Wrapper to do all work in verify_all_objects.
    """
    stat_obj = get_stat_object(row_obj["ceph_bucket"], row_obj["ceph_name"])
    print(dir(stat_obj))
    print(stat_obj.compute_md5())
    print(stat_obj.etag)
    return True

def verify_all_objects(row_objs):
    """Loop over all the objects to verify from the database. Possibly
    multithreaded.
    """
    retval = True
    for row_obj in row_objs:
        retval = retval and verify_all_objects_worker(row_obj)
    return retval

if __name__ == '__main__':

    configure_app_log(2, logfile="./verify.log", journal='auto')

    argparser = argparse.ArgumentParser(
                description="Verify objects in ceph, track verification in database")
    argparser.add_argument("-b", "--bucket", required=False,
                       help="Bucket name eg 'idigbio-images-prod'")
    argparser.add_argument("-e", "--etag", required=False,
                       help="Verify only this one etag")
    argparser.add_argument("-s", "--start", required=False,
                       help="Start date for when ceph object was created eg '2010-02-23'")
    argparser.add_argument("-d", "--end", required=False,
                       help="End date for when ceph object was created eg '2018-01-01'")
    argparser.add_argument("-c", "--count", required=False, default=10,
                       help="How many to verify, default 10")
    argparser.add_argument("-n", "--name", required=False,
                       help="Verify only this one name")
    argparser.add_argument("-r", "--reverify", required=False,
                       help="Reverify objects that already have been verified")
    argparser.add_argument("-t", "--test", required=False,
                       help="Don't update database with results, just print to stdout")
    argparser.add_argument("-p", "--processes", required=False, default=1,
                       help="How many processing to use verifying objects, default 1")
    args = argparser.parse_args()
    #print(args)

    row_objs = get_row_objs_from_db(args)
    print(row_objs)

    print(verify_all_objects(row_objs))

    apidbpool.closeall()
