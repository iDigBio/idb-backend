import sys
from idb.helpers.logging import getLogger, configure_app_log
from idb.helpers.storage import IDigBioStorage
from subprocess import check_output, CalledProcessError
import hashlib
import json

logger = getLogger("reconstruct")

from idb.postgres_backend.db import PostgresDB
#store = IDigBioStorage()

TMP_DIR = "/tmp/{0}".format(sys.argv[0])


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


def stat_obj_to_file_names(stat_obj):
    # iterate over the file parts list in a stats object and return
    # an in-order list of file name substrings that should be unique
    # to look for on disk by assembling the metadata
    files = []


    for o in stat_obj["manifest"]["objs"][1::2]: #objs is a list that alternates byte and dict
        # This chunk is only present for file parts > 1, first part has no ns
        if o["loc"]["key"]["ns"]:
            ns = "\u{0}".format(o["loc"]["key"]["ns"])
        else:
            ns = ""

        files.append(''.join((
                             o["loc"]["bucket"]["marker"],
                             "\u",
                             ns,
                             o["loc"]["key"]["name"].replace("_", "\u", 99),
                             "__" # characters on end serve to terminate the pattern since many ceph objects have the same name prefix eg foo and (2) foo.jpg in different buckets
                            ))
                    )
    # FIXME: need to fix the first part, should not include the ns key
    return files

def find_files(patterns):
    # Using the index of all files on all servers, return a list of dicts with
    # information about that file is on disk(s)
    files = []
    q = """SELECT
            server, fullname, unk3
           FROM ceph_server_files
           WHERE
            filename LIKE %s
        """

    with PostgresDB() as db:
        for pattern in patterns:
            # When in doubt, add more backslashes!
            rows = db.fetchall(q, ("{0}%".format(pattern.replace('\\','\\\\')),))

            copies = []
            for c in rows:
                copies.append(c)

            files.append(copies)

    return files

def retrieve_files_from_server(files, servers):
    return False


def rebuild_from_files(ceph_name, outputdir, files, stat_obj):
    # Do the rebuild and verification of a file from its parts
    if retrieve_files_from_server:
        # concatenate files
        # check md5sum
        # move to output dir
        return True
    return False

if __name__ == '__main__':
    if len(sys.argv) == 4:
        ceph_bucket = sys.argv[1]
        ceph_name = sys.argv[2]
        output_dir = sys.argv[3]
    else:
        print("Usage: {0} <bucket> <file name> <output dir>".format(sys.argv[0]))
        exit(64)

    stat_obj = get_file_parts(ceph_bucket, ceph_name)
    #print(stat_obj)
    patterns = stat_obj_to_file_names(stat_obj)
    print(patterns)
    files_sources = find_files(patterns)
    print(files_sources)

#    rebuild_from_files(ceph_name, output_dir, files, stat_obj)
