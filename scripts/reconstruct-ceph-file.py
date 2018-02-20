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

# data structure of parts_obj, in order by part to be assembled
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

def find_files_on_servers(parts_obj):
    # Using the index of all files on all servers, return a list of dicts with
    # information about that file is on disk(s)
    q = """SELECT
            server, fullname, filename, size
           FROM ceph_server_files
           WHERE
            filename LIKE %s
        """

    with PostgresDB() as db:
        for i, part in enumerate(parts_obj):
            # When in doubt, add more backslashes!
            rows = db.fetchall(q, ("{0}%".format(part["pattern"].replace('\\','\\\\')),))

#            copies = []
#            for c in rows:
#                copies.append(c)

            parts_obj[i]["copies"] = rows

    return parts_obj

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
    parts_obj = stat_obj_to_parts_obj(stat_obj)
    print(parts_obj)
    parts_obj = find_files_on_servers(parts_obj)
    print(parts_obj)

#    rebuild_from_files(ceph_name, output_dir, files, stat_obj)
