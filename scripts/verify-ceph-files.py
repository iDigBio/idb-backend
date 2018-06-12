# Verify objects in ceph by downloading and checksumming them and making
# sure whats in the database matches what ceph is serving.
# Track status and state with the ceph_objects table in the databse.
import gevent
from gevent import monkey, pool
monkey.patch_all()

import os
import sys
import shutil
import argparse
import hashlib
import traceback

import datetime

from httplib import HTTPException
from socket import error as socket_error


from idb.postgres_backend import apidbpool
from idb.helpers.logging import getLogger, configure_app_log
from idb.helpers.storage import IDigBioStorage
from boto.exception import S3ResponseError

TMP_DIR = os.path.join("/tmp", os.path.basename(sys.argv[0]))
STORAGE_HOST = "10.13.44.93:7480"

TEST = False
DELETE = False
STASH = None
DELETED = []

logger = getLogger("verify-ceph-files")

def check_args_and_set_global_flags(args):
    """Check the command-line arguments and set some global flags to control
    processing."""

    global DELETE
    global TEST
    global STASH

    try:
        if args["names_from_file"] is not None:
            logger.error("'--names-from-file' not yet implemented.  Exiting...")
            # Future: verify the file exits and is readable
            raise SystemExit
    except:
        raise

    if "stash_and_delete" in args:
        if args["stash_and_delete"] is not None:
            STASH = args["stash_and_delete"]
            DELETE = True
    if "stash" in args:
        if args["stash"] is not None:
            STASH = args["stash"]
            DELETE = False
    if STASH is not None:
        if not (os.path.isdir(STASH)):
            logger.error("Specified stash directory {0} does not exist. Aborting.".format(STASH))
            raise SystemExit

    TEST = args["test"]
    if TEST:
        logger.warn("TEST mode. Will not update the database or delete objects from ceph.")

    logger.info("Processing with the following arguments: {0}".format(args))

    # respect count when processing multiple objects
    if (args["any"]) or (args["names_from_file"]):
        logger.info("using COUNT = {0}. Use '--count' option ".format(args["count"]) + \
                    "to increase COUNT if you wish to process more than {0} objects.".format(args["count"]))

def output_deleted():
    # We should probably make a logfile instead?
    if len(DELETED) == 0:
        logger.info("No objects Deleted.")
    else:
        logger.info("DELETED objects list follows...")
        logger.info("************************************************")
        for each in DELETED:
            print (each)
        logger.info("************************************************")

def get_key_object(bucket, name):
    """Get a key object from Ceph for the requested object.

    Note that most of the metadata with the key won't be populated
    until after it has been fetched.
    """
    global STORAGE_HOST
    storage = IDigBioStorage(host=STORAGE_HOST)
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
    #wheres.append("ceph_date IS NULL") # for testing date updates

    if args["start"]:
        wheres.append("ceph_date>=%(start)s")
    if args["end"]:
        wheres.append("ceph_date<=%(end)s")
    if args["name"]:
        wheres.append("ceph_name like %(name)s")
    if args["bucket"]:
        wheres.append("ceph_bucket=%(bucket)s")
    if args["reverify"]:
        wheres.append("ver_status=%(reverify)s")
    else:
        wheres.append("ver_status IS NULL")

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
    """Download an object and check it against the expected metadata.

    Return is a string status of result of checking the file:

    verified - Object downloads and all available data matches
    stashed - Object is verified and a copy has been kept in stash directory
    timeout - Download times out, probably due to file being truncated
    nosuchkey - Object does not exist, 404 error when downloading
    invalid - Some of the metadata does not match
    failed - No longer used, when this function was boolean this was False
    """

    storage = IDigBioStorage(host=STORAGE_HOST)

    try:
        if not os.path.exists(TMP_DIR):
            os.makedirs(TMP_DIR)
        fn = os.path.join(TMP_DIR, key_obj.name)

        logger.debug("Fetching file {0}:{1}".format(key_obj.bucket.name, key_obj.name))
        storage.get_contents_to_filename(key_obj, fn)
        md5 = calc_md5(fn)
        size = os.stat(fn).st_size
    except (S3ResponseError) as ex:
        if "NoSuchKey" in str(ex):
            logger.error("No such key when getting {0}:{1}".format(key_obj.bucket.name, key_obj.name))
            return "nosuchkey"
        else:
            logger.error("Exception while attempting to get file {0}:{1} {2}".format(key_obj.bucket.name, key_obj.name, traceback.format_exc()))
#            raise
            return "S3ResponseError"
    except (HTTPException, socket_error) as ex:
        # Timeout can be controlled by /etc/boto.cfg - see http://boto.cloudhackers.com/en/latest/boto_config_tut.html
        logger.error("Socket timeout when getting {0}:{1}, file is probably corrupt in ceph".format(key_obj.bucket.name, key_obj.name))
        if os.path.exists(fn):
            os.unlink(fn)
        return "timeout"
    except Exception as ex:
        if "503 Service Unavailable" in str(ex):
            logger.error("Service unavailable getting {0}:{1}".format(key_obj.bucket.name, key_obj.name))
            return "503Error"
        else:
            logger.error("Exception while attempting to get file {0}:{1} {2}".format(key_obj.bucket.name, key_obj.name, traceback.format_exc()))
            raise

    # The db may have partial information so we need to support it being
    # empty, but if it exists, it should match. Use logging to say what's
    # wrong with file, maintain a return value if anything fails.
    retval = False

    if not retval and (not size == key_obj.size):
        logger.error("File size {0} does not match ceph size {1} for {2}:{3}".format(
                     size, key_obj.size, key_obj.bucket.name, key_obj.name))
        retval = "invalid"

    if not retval and (row_obj["ceph_bytes"] and (not size == row_obj["ceph_bytes"])):
        logger.error("File size {0} does not match db size {1} for {2}:{3}".format(
                     size, row_obj["ceph_bytes"], key_obj.bucket.name, key_obj.name))
        retval = "invalid"

    if not retval and (not md5 == key_obj.etag[1:-1]): # etag is wraped in ""
        logger.error("File md5 {0} does not match ceph etag {1} for {2}:{3}".format(
                     md5, key_obj.etag[1:-1], key_obj.bucket.name, key_obj.name))
        retval = "invalid"

    # db etag has extra '-' chars
    if not retval and (row_obj["ceph_etag"] and (not md5 == row_obj["ceph_etag"].replace('-',''))):
        logger.error("File md5 {0} does not match db etag {1} for {2}:{3}".format(
                     md5, row_obj["ceph_etag"], key_obj.bucket.name, key_obj.name))
        retval = "invalid"


    if not retval:
        logger.debug("Object {0}:{1} verified".format(key_obj.bucket.name, key_obj.name))
        #global args
        if STASH and stash_file(fn, key_obj):
            retval = "stashed"
            if DELETE and not TEST:
                try:
                    logger.info("** Here we would be able to delete the object! **")
                    DELETED.append(key_obj.name)
                except:
                    logger.error("Unable to delete object {0}:{1}.".format(
                            key_obj.bucket.name, key_obj.name))
                    
        else:
            retval = "verified"
    else:
        logger.warn("Object {0}:{1} failed verification".format(key_obj.bucket.name, key_obj.name))

    try:
        os.unlink(fn)
    except:
        pass

    return retval


def stash_file(fn, key_obj):
    #global args

    dest_dir = os.path.join(STASH, key_obj.bucket.name)
    if not os.path.exists(dest_dir):
        os.makedirs(dest_dir)
    dest_file = os.path.join(dest_dir, key_obj.name)

    try:
        shutil.copyfile(fn, dest_file)
        logger.debug("Stashed file {0}:{1} in {2}".format(
                key_obj.bucket.name, key_obj.name, dest_file))
        return True
    except:
        logger.error("Failed to stash file {0}:{1} in {2}: {3}".format(
                key_obj.bucket.name, key_obj.name, STASH, traceback.format_exc()))
        return False


def update_db(row_obj, key_obj, status):
    """Some db records are incomplete due to the original db being used
    for backups and not a full accounting of object properties, backfill
    any missing information into the database. Verify status is the result of this check.
    """

    if TEST:
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
        vals["status"] = status

        if status == "verified":
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

        # Seems like if object does not transfer fully, etag is not set?
        if (status == "verified") and not row_obj["ceph_etag"]:
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
    status = verify_object(row_obj, key_obj)
    return update_db(row_obj, key_obj, status) and ((status == "verified") or (status == "stashed"))

def verify_all_objects(row_objs, processes):
    """Loop over all the objects to verify from the database. Possibly
    multithreaded.
    """

    logger.info("Begin verify objects...")
#    fail = 0
#    succeed = 0
#    for row_obj in row_objs:
#        if verify_all_objects_worker(row_obj):
#            succeed += 1
#        else:
#            fail += 1
#    return (fail, succeed)

    p = pool.Pool(processes)
    results = p.imap_unordered(verify_all_objects_worker, row_objs)
    return sum(results)

if __name__ == '__main__':

    # this enables debug level logging
    configure_app_log(2, logfile="./verify.log", journal='auto')

    argparser = argparse.ArgumentParser(
                description="Verify objects in ceph, track verification in database.")
    argparser.add_argument("--bucket", "-b",
                           required=False,
                           help="Bucket name eg 'idigbio-images-prod'.")
#    argparser.add_argument("-e", "--etag", required=False,
#                       help="Verify only this one etag")
    argparser.add_argument("--start", "-s", 
                           required=False, metavar='DATE_START',
                           help="Start date for when ceph object was created eg '2010-02-23'.")
    argparser.add_argument("--end", "-e",
                           required=False, metavar='DATE_END',
                           help="End date for when ceph object was created eg '2018-01-01'.")
    argparser.add_argument("--count", "-c",
                           required=False, type=int, default=10,
                           help="How many to verify, default 10.")
    argparser.add_argument("--reverify", "-r",
                           required=False, action='store_true',
                           help="Reverify objects that have the specified status.")
    argparser.add_argument("--test", "-t",
                           required=False, action='store_true',
                           help="Don't update database with verify results or delete any ceph objects.")
    argparser.add_argument("--processes", "-p",
                           required=False, type=int, default=1, metavar='NUM_PROCESSES',
                           help="How many processing to use verifying objects, default 1.")
    # Use either --stash or --stash-and-delete
    stashgroup = argparser.add_mutually_exclusive_group()
    stashgroup.add_argument("--stash", "-g",
                             required=False, metavar='STASH_DIRECTORY_PATH',
                             help="If verified, stash the file in given dir and mark ver_status as 'stashed'.")
    stashgroup.add_argument("--stash-and-delete",
                            required=False, metavar='STASH_DIRECTORY_PATH',
                            help="Same as --stash, except delete each object in ceph storage after it is verified and stashed.")

    # Script will no longer run without at least one argument
    group = argparser.add_mutually_exclusive_group(required=True)
    group.add_argument("--name", "-n",
                       help="Verify only this one name.")
    group.add_argument("--names-from-file",
                       metavar='NAMES_INPUT_FILEPATH',
                       help="Read ceph names from a file, one name per line.")
    group.add_argument("--any", "-a", action='store_true',
                       help="Select any names. This was default behavior in previous versions of this script.")
    
    args = vars(argparser.parse_args()) # convert namespace to dict
    #print(args)

    check_args_and_set_global_flags(args)

    #print(iDigBioStorage.boto.config)

    row_objs = get_row_objs_from_db(args)
    #print(row_objs)

    verified = verify_all_objects(row_objs, int(args["processes"]))
    if DELETE:
        output_deleted()
        logger.info("Checked "
                    "{0} objects, Verified {1} objects, Deleted {2} objects from ceph,"
                    "Stashed in '{3}'.".format(len(row_objs), verified, len(DELETED), STASH))
    elif STASH:
        logger.info("Checked "
                    "{0} objects, Verified {1} objects, Stashed in '{2}'".format
                    (len(row_objs), verified, STASH))
    else:
        logger.info("Checked {0} objects, Verified {1} objects".format(
                len(row_objs), verified))


    apidbpool.closeall()
