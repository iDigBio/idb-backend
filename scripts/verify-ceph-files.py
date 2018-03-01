# Verify objects in ceph by downloading and checksumming them and making
# sure whats in the database matches what ceph is serving.
# Track status and state with the ceph_objects table in the databse.
import os
import sys
import argparse
import hashlib

from idb.postgres_backend import apidbpool
from idb.helpers.logging import getLogger, configure_app_log
from idb.helpers.storage import IDigBioStorage

TMP_DIR = os.path.join("/tmp", os.path.basename(sys.argv[0]))

logger = getLogger("verify-ceph-files")
storage = IDigBioStorage()


def get_key_object(bucket, name):
    """Get a key object from Ceph for the requested object.

    Note that most of the metadata with the key won't be populated
    until after it has been fetched.
    """
    #global storage
    logger.debug("Retreiving key for {0}:{1}".format(bucket, name))
    key = storage.get_key(name, bucket)
    return key

def get_row_objs_from_db(args):
    """Get a list of objects to verify from the database based on user's
    passed arguments.
    """
    cols = ["ceph_bucket", "ceph_name", "ceph_date", "ceph_bytes",
            "ver_status", "ver_last_success", "ver_last_failure"]

    wheres = []
    wheres.append("length(ceph_name)>=10")
    wheres.append("ceph_bytes IS NOT NULL") # for initial testing
    wheres.append("ver_status IS NULL") # replace w/ argument soon

    if args["name"]:
        wheres.append("ceph_name=%(name)s")
    if args["bucket"]:
        wheres.append("ceph_bucket=%(bucket)s")

    rows = apidbpool.fetchall("""SELECT {0} FROM ceph_objects WHERE
                                  {1}
                                  LIMIT %(count)s""".format(','.join(cols),
                                                            ' AND '.join(wheres)),
                                  args)
    row_objs = []
    for row in rows:
        row_objs.append(dict(zip(cols, row)))
    return row_objs

def calc_md5(fn):
    with open(fn, "rb") as f:
        f_md5 = hashlib.md5()
        for chunk in iter(lambda: f.read(4096), b""):
            f_md5.update(chunk)
        return f_md5.hexdigest()

def verify_object(row_obj, key_obj):
    """Download an object and check it against the expected metadata."""
    global TMP_DIR
    try:
        if not os.path.exists(TMP_DIR):
            os.makedirs(TMP_DIR)
        fn = os.path.join(TMP_DIR, key_obj.name)

        logger.debug("Fetching file {0}".format(key_obj.name))
        storage.get_contents_to_filename(key_obj, fn)
        md5 = calc_md5(fn)

        # The db may have partial information so we need to support it being
        # empty, but if it exists, it should match. Build an array of failed
        # check tags similar to what we do for iDigBio data quality.

        # HERE!

        print(key_obj.__dict__)
        #os.unlink(fn)
        return ()
    except:
        logger.error("Exception while attempting to get file {0}".format(key_obj.name))
        raise
        return False

def update_db_metadata(row_obj, key_obj, verify_status=False):
    """Some db records are incomplete due to the original db being used
    for backups and not a full accounting of object properties, backfill
    any missing information into the database. Verify status is the result of this check.
    """
    return True

def verify_all_objects_worker(row_obj):
    """Wrapper to do all work in verify_all_objects.
    """
    key_obj = get_key_object(row_obj["ceph_bucket"], row_obj["ceph_name"])
    if verify_object(row_obj, key_obj):
        return update_db_metadata(row_obj, key_obj) # add test argument
    else:
        return False

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
#    argparser.add_argument("-e", "--etag", required=False,
#                       help="Verify only this one etag")
#    argparser.add_argument("-s", "--start", required=False,
#                       help="Start date for when ceph object was created eg '2010-02-23'")
#    argparser.add_argument("-d", "--end", required=False,
#                       help="End date for when ceph object was created eg '2018-01-01'")
    argparser.add_argument("-c", "--count", required=False, default=10,
                       help="How many to verify, default 10")
    argparser.add_argument("-n", "--name", required=False,
                       help="Verify only this one name")
#    argparser.add_argument("-r", "--reverify", required=False,
#                       help="Reverify objects that already have been verified")
#    argparser.add_argument("-t", "--test", required=False,
#                       help="Don't update database with results, just print to stdout")
#    argparser.add_argument("-p", "--processes", required=False, default=1,
#                       help="How many processing to use verifying objects, default 1")
    args = vars(argparser.parse_args()) # convert namespace to dict
    #print(args)

    row_objs = get_row_objs_from_db(args)
    print(row_objs)

    print(verify_all_objects(row_objs))

    apidbpool.closeall()
