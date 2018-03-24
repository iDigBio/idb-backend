import sys
import os
import shutil
import json
import datetime
import traceback
import argparse
import paramiko
from idb.helpers.logging import getLogger, configure_app_log
from idb.helpers.storage import IDigBioStorage
import idb.config
from subprocess import check_output, CalledProcessError
import hashlib
from logging import ERROR

from idb.postgres_backend import apidbpool

# are these needed?
#import idb.postgres_backend as pg_backend
#from idb.postgres_backend.db import PostgresDB



TMP_DIR = "/tmp/{0}".format(os.path.basename(sys.argv[0]))

# Data structure of parts_obj, stored in order by part to be assembled
#
# [
#  { "pattern": "foo",
#    "copies": [
#                { "server": "c15node1",
#                  "fullpath": "/.../...pattern...",
#                  "filename": "...pattern...",
#                  "size": 123,
#                }, ...
#              ]
#    "localpath": "/tmp/fullname"
#  }, ...
# ]


def get_row_objs_from_db(args):
    """Get a list of objects to reconstruct from the database.

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
    if args["verify"]:
        wheres.append("ver_status=%(verify)s")
    else:
        wheres.append("ver_status='timeout'")
    if args["rereconstruct"]:
        wheres.append("rest_status=%(rereconstruct)s")
    else:
        wheres.append("rest_status IS NULL")

    rows = apidbpool.fetchall("""SELECT {0} FROM ceph_objects WHERE
                                  {1}
                                  LIMIT %(count)s""".format(','.join(cols),
                                                            ' AND '.join(wheres)),
                                  args)
    row_objs = []
    for row in rows:
        row_objs.append(dict(zip(cols, row)))
    return row_objs


def get_file_from_server(server, remote_fn, local_fn):
    logger.debug("Getting file from {0}:{1} to {2}".format(
        server, remote_fn, local_fn))

    try:
        ssh = paramiko.SSHClient()
        ssh.load_host_keys(os.path.expanduser(os.path.join("~", ".ssh", "known_hosts")))
        privatekeyfile = os.path.expanduser(os.path.join("~", ".ssh", "id_rsa"))
        myid = paramiko.RSAKey.from_private_key_file(privatekeyfile)
        ssh.connect(server, username="root", pkey=myid)
        sftp = ssh.open_sftp()
        sftp.get(remote_fn, local_fn)
    except:
        raise
    finally:
        try:
            sftp.close()
        except:
            pass
        try:
            ssh.close()
        except:
            pass

    return os.path.exists(local_fn)


def get_file_parts(ceph_bucket, ceph_name):
    # run this on idb-rgw1
    # radosgw-admin object stat --object=c895d988bc4dfcb2d9f7f4cb4bf1d430 --bucket=idigbio-images-prod
    # and return the parsed JSON as a stat object
    try:
        r = check_output( ("/usr/bin/ssh "
                          "root@idb-rgw1 radosgw-admin object stat --bucket={0} "
                           "--object={1}".format(ceph_bucket, ceph_name)),
                         shell=True
                    )
    except Exception as e:
        logger.error("Unable to retrieve info for {0} {1}".format(ceph_bucket, ceph_name))
        raise

    try:
        stat_obj = json.loads(r)
    except:
        logger.error("Unable to parse JSON from {0}".format(r[0:50]))
        raise

    return stat_obj


def stat_obj_to_parts_obj(stat_obj):
    # iterate over the file parts list in a stats object and return
    # an in-order list of file name substrings that should be unique
    # to look for on disk by assembling the metadata
    parts_obj = []


    for o in stat_obj["manifest"]["objs"][1::2]: #objs is a list that alternates byte and dict
        # This chunk is only present for file parts > 1, first part has no ns
        if o["loc"]["key"]["ns"]:
            ns = "\u{0}".format(o["loc"]["key"]["ns"])
        else:
            ns = ""

        parts_obj.append({"pattern":
                             ''.join((
                             o["loc"]["bucket"]["marker"],
                             "\u",
                             ns,
                             o["loc"]["key"]["name"].replace("_", "\u", 99),
                             "__" # characters on end serve to terminate the pattern since many ceph objects have the same name prefix eg foo and (2) foo.jpg in different buckets
                            ))
                          }
                         )
    return parts_obj


def find_parts_on_servers(parts_obj):
    # Using the index of all files on all servers, return a list of dicts with
    # information about that file is on disk(s)

    cols = ["server", "fullname", "filename", "size"]
    q = """SELECT
            {}
           FROM ceph_server_files
           WHERE
            filename LIKE %s
        """.format(','.join(cols))

    for i, part in enumerate(parts_obj):
        logger.debug("Looking up filenames for {0}".format(part["pattern"]))

        # When in doubt, add more backslashes!
        rows = apidbpool.fetchall(q, ("{0}%".format(part["pattern"].replace('\\','\\\\')),))

        copies = []
        for c in rows:
            copies.append(dict(zip(cols, c)))
        parts_obj[i]["copies"] = copies

    return parts_obj


def get_file_parts_from_servers(parts_obj):
    # download one of the parts
    if not os.path.exists(TMP_DIR):
        os.mkdirs(TMP_DIR)

    for i, part in enumerate(parts_obj):
        c = part["copies"][0] # examine re-trying additional copies
        local_fn = os.path.join(TMP_DIR, c["filename"])
        if get_file_from_server(c["server"],
                                 c["fullname"],
                                 local_fn):
            parts_obj[i]["localpath"] = local_fn

    return parts_obj


def verify_file(fn, size, md5):
    if os.stat(fn).st_size == size:
        with open(fn, "rb") as f:
            f_md5 = hashlib.md5()
            for chunk in iter(lambda: f.read(4096), b""):
                f_md5.update(chunk)
        #print(f_md5.hexdigest())
        return f_md5.hexdigest() == md5
    else:
        #print(os.stat(fn).st_size + " " + size)
        return False


def reconstruct_file(parts_obj, stats_obj, output_dir):
    # Concatenate all the parts and verify them, move to output dir
    # and clean up parts in tmp dir

    # use the bucket name from the first object
    dest_dir = os.path.join(output_dir,
                  stats_obj["manifest"]["objs"][1]["loc"]["bucket"]["name"])
    if not os.path.exists(dest_dir):
        os.makedirs(dest_dir)
    dest_file = os.path.join(dest_dir, stats_obj["name"])

    with open(dest_file, "wb") as dest:
        for part in parts_obj:
            with open(part["localpath"], "rb") as src:
                shutil.copyfileobj(src, dest)

    if verify_file(dest_file, stats_obj["size"], stats_obj["etag"]):
        for part in parts_obj:
            os.unlink(part["localpath"])
        return True
    else:
        return False


def do_a_file(ceph_bucket, ceph_name, output_dir):
    logger.info("Starting reconstruction of {0}/{1}".format(ceph_bucket, ceph_name))
    return True

    try:
        stat_obj = get_file_parts(ceph_bucket, ceph_name)
        #print(stat_obj)
        parts_obj = stat_obj_to_parts_obj(stat_obj)
        #print(parts_obj)
        parts_obj = find_parts_on_servers(parts_obj)
        #print(parts_obj)
        parts_obj = get_file_parts_from_servers(parts_obj)
        #print(parts_obj[0])
        return reconstruct_file(parts_obj, stat_obj, output_dir)
    except Exception as e:
        logger.error("Exception while reconstructing {0}/{1} {2}".format(
                     ceph_bucket, ceph_name, traceback.format_exc()))
        raise
        return False

def update_db(ceph_bucket, ceph_name, status):
    global test

    if test:
        logger.debug("Skipping database udpate for {0}/{1}".format(
                     ceph_bucket, ceph_name))
    else:
        logger.debug("Updating database for {0}/{1}".format(
                     ceph_bucket, ceph_name))
        cols = []
        vals = {"ceph_name": ceph_name, "ceph_bucket": ceph_bucket}


        cols.append("rest_status=%(status)s")
        vals["status"] = status

        if status == "reconstructed":
            cols.append("rest_last_success=%(timestamp)s")
        else:
            cols.append("rest_last_failure=%(timestamp)s")
        vals["timestamp"] = '{:%Y-%m-%d %H:%M:%S}'.format(datetime.datetime.now())

        apidbpool.execute("""UPDATE ceph_objects
            SET {0}
            WHERE
            ceph_name=%(ceph_name)s
            AND ceph_bucket=%(ceph_bucket)s
        """.format(",".join(cols)), vals)

def worker(ceph_bucket, ceph_name, outdir):
        if do_a_file(ceph_bucket,  ceph_name, outdir):
            status = "reconstructed"
        else:
            status = "broken"
        update_db(r["ceph_bucket"], r["ceph_name"], status)


if __name__ == '__main__':

    configure_app_log(2, logfile="./reconstruct.log", journal="auto")
    getLogger('paramiko').setLevel(ERROR)
    logger = getLogger("reconstruct")

    argparser = argparse.ArgumentParser(
                    description="Reconstruct a ceph object from files on disk")
    argparser.add_argument("-b", "--bucket", required=False,
                   help="Bucket name eg 'idigbio-images-prod'")
    argparser.add_argument("-n", "--name", required=False,
                       help="Verify only this one name")
    argparser.add_argument("-o", "--outdir", required=True,
                       help="Root directory to write subpath bucket/name to")
    argparser.add_argument("-s", "--start", required=False,
                       help="Start date for when ceph object was created eg '2010-02-23'")
    argparser.add_argument("-d", "--end", required=False,
                       help="End date for when ceph object was created eg '2018-01-01'")
    argparser.add_argument("-c", "--count", required=False, default=10,
                       help="How many to verify, default 10")
    argparser.add_argument("-r", "--rereconstruct", required=False,
                       help="Re-reconstruct objects that have the specified reconstruction status")
    argparser.add_argument("-v", "--verify", required=False,
                       help="Re-reconstruct objects that have the specified verification status")
    argparser.add_argument("-t", "--test", required=False,
                       help="Don't update database with results, just reconstruct")
    argparser.add_argument("-p", "--processes", required=False, default=1,
                       help="How many processing to use reconstructing objects, default 1")



    args = vars(argparser.parse_args()) # convert namespace to dict

    test = True if args["test"] else False


    rows = get_row_objs_from_db(args)
    for r in rows:
        worker(r["ceph_bucket"],  r["ceph_name"], args["outdir"])
