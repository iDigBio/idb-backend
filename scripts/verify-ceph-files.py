# Verify objects in ceph by downloading and checksumming them and making
# sure whats in the database matches what ceph is serving.
# Track status and state with the ceph_objects table in the databse.
import os
import sys
import argparse
import hashlib
#import time

import datetime

from httplib import HTTPException
from socket import error as socket_error


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
    """Get a list of objects to verify from the database.

    Uses the user's arguments to build the query for which objects
    to verify.
    """
    cols = ["ceph_bucket", "ceph_name", "ceph_date", "ceph_bytes", "ceph_etag",
            "ver_status", "ver_last_success", "ver_last_failure"]

    wheres = []
    wheres.append("length(ceph_name)>=10")
    #wheres.append("ceph_bytes IS NOT NULL") # for initial testing
    wheres.append("ver_status IS NULL") # replace w/ argument soon
    #wheres.append("ceph_date IS NULL") # for testing date updates

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
    """Calculate the md5 hash of a file on disk."""
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

        logger.debug("Fetching file {0}:{1}".format(key_obj.bucket.name, key_obj.name))
        storage.get_contents_to_filename(key_obj, fn)
        md5 = calc_md5(fn)
        size = os.stat(fn).st_size
    except (HTTPException, socket_error) as ex:
        # Timeout can be controlled by /etc/boto.cfg - see http://boto.cloudhackers.com/en/latest/boto_config_tut.html
        logger.error("Socket timeout when getting {0}:{1}, file is probably corrupt in ceph".format(key_obj.bucket.name, key_obj.name))
        if os.path.exists(fn):
            os.unlink(fn)
        return False
    except:
        logger.error("Exception while attempting to get file {0}:{1}".format(key_obj.bucket.name, key_obj.name))
        raise

    # The db may have partial information so we need to support it being
    # empty, but if it exists, it should match. Use logging to say what's
    # wrong with file, maintain a return value if anything fails.
    retval = True

    if not size == key_obj.size:
        logger.error("File size {0} does not match ceph size {1} for {2}:{3}".format(
                     size, key_obj.size, key_obj.bucket.name, key_obj.name))
        retval = False

    if row_obj["ceph_bytes"] and (not size == row_obj["ceph_bytes"]):
        logger.error("File size {0} does not match db size {1} for {2}:{3}".format(
                     size, db_obj["ceph_bytes"], key_obj.bucket.name, key_obj.name))
        retval = False

    if not md5 == key_obj.etag[1:-1]: # etag is wraped in ""
        logger.error("File md5 {0} does not match ceph etag {1} for {2}:{3}".format(
                     md5, key_obj.etag[1:-1], key_obj.bucket.name, key_obj.name))
        retval = False

    if row_obj["ceph_etag"] and (not md5 == row_obj["ceph_etag"]):
        logger.error("File md5 {0} does not match db etag {1} for {2}:{3}".format(
                     md5, row_obj["ceph_etag"], key_obj.bucket.name, key_obj.name))
        retval = False

    os.unlink(fn)

    if retval:
        logger.debug("Object {0}:{1} verified".format(key_obj.bucket.name, key_obj.name))
    else:
        logger.warn("Object {0}:{1} failed verification".format(key_obj.bucket.name, key_obj.name))
    return retval

def update_db(row_obj, key_obj, verified):
    """Some db records are incomplete due to the original db being used
    for backups and not a full accounting of object properties, backfill
    any missing information into the database. Verify status is the result of this check.
    """
    global test

    if test:
        logger.debug("Skipping metadata update for {0}:{1}".format(
                     key_obj.bucket.name, key_obj.name))
        return True
    else:
        logger.debug("Updating database record for {0}:{1}".format(
                     key_obj.bucket.name, key_obj.name))
        cols = []
        vals = {"ceph_name": row_obj["ceph_name"],
                "ceph_bucket": row_obj["ceph_bucket"]}

        cols.append("ver_status=%(status)s")
        vals["status"] = "verified" if verified else "failed"

        if verified:
            cols.append("ver_last_success=%(timestamp)s")
        else:
            cols.append("ver_last_failure=%(timestamp)s")
        vals["timestamp"] = '{:%Y-%m-%d %H:%M:%S}'.format(datetime.datetime.now())

        # Even if the obj does not verify, update the db with what's in ceph
        if not row_obj["ceph_date"]:
            cols.append("ceph_date=%(last_modified)s")
            vals["last_modified"] = key_obj.last_modified

        if not row_obj["ceph_bytes"]:
            cols.append("ceph_bytes=%(size)s")
            vals["size"] = key_obj.size

        if not row_obj["ceph_etag"]:
            cols.append("ceph_etag=%(etag)s")
            vals["etag"] = key_obj.etag[1:-1]

        return apidbpool.execute("""UPDATE ceph_objects
                             SET {0}
                             WHERE
                             ceph_name=%(ceph_name)s 
                             AND ceph_bucket=%(ceph_bucket)s""".format(
                             ",".join(cols)),
                          vals)

def verify_all_objects_worker(row_obj):
    """Wrapper to do all work in verify_all_objects.
    """
    key_obj = get_key_object(row_obj["ceph_bucket"], row_obj["ceph_name"])
    verified = verify_object(row_obj, key_obj)
    return update_db(row_obj, key_obj, verified) and verified

def verify_all_objects(row_objs):
    """Loop over all the objects to verify from the database. Possibly
    multithreaded.
    """
    fail = 0
    succeed = 0
    for row_obj in row_objs:
        if verify_all_objects_worker(row_obj):
            succeed += 1
        else:
            fail += 1
    return (fail, succeed)

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
    argparser.add_argument("-t", "--test", required=False,
                       help="Don't update database with results, just print to stdout")
#    argparser.add_argument("-p", "--processes", required=False, default=1,
#                       help="How many processing to use verifying objects, default 1")
    args = vars(argparser.parse_args()) # convert namespace to dict
    #print(args)

    # Make test global
    if args["test"]:
        test = True
    else:
        test = False

    #print(iDigBioStorage.boto.config)

    row_objs = get_row_objs_from_db(args)
    #print(row_objs)

    fail, succeed = verify_all_objects(row_objs)
    logger.info("Checked {0} objects, {1} failed, {2} succeeded".format(
                 len(row_objs), fail, succeed))

    apidbpool.closeall()
