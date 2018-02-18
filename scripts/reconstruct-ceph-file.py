import sys
from idb.helpers.logging import getLogger, configure_app_log
from idb.helpers.storage import IDigBioStorage
from subprocess import check_output
import hashlib
import json

logger = getLogger("restore")
store = IDigBioStorage()

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
        print("Unable to retrieve info for {0} {1}".format(ceph_bucket, ceph_name))
        raise

    try:
        stat_obj = json.loads(r)
    except:
        print("Unable to parse JSON from {0}".format(r[0:50]))
        raise

    return stat_obj


def stat_obj_to_file_names(stat_obj):
    # iterate over the file parts list in a stats object and return
    # an in-order list of file name substrings that should be unique
    # to look for on disk by assembling the metadata
    files = []
    for o in stat_obj["manifest"]["objs"][1::2]: #objs is a list that alternates byte and dict
        files.append(''.join((
                             o["loc"]["bucket"]["marker"],
                             "\u\u",
                             o["loc"]["key"]["ns"],
                             o["loc"]["key"]["name"].replace("_", "\u", 99),
                             "__"
                            ))
                    )
    # FIXME: need to fix the first part, should not include the ns key
    return files

def find_files(files):
    # Using the index of all files on all servers, return a nested list of
    # server names for each file
    return [ [] ]

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
    files = stat_obj_to_file_names(stat_obj)
    print(files)

    rebuild_from_files(ceph_name, output_dir, files, stat_obj)
