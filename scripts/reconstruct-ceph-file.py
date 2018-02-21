import sys
import os
import shutil
import json
import paramiko
from idb.helpers.logging import getLogger, configure_app_log
from idb.helpers.storage import IDigBioStorage
from subprocess import check_output, CalledProcessError
import hashlib


logger = getLogger("reconstruct")

from idb.postgres_backend.db import PostgresDB
#store = IDigBioStorage()

TMP_DIR = "/tmp/{0}".format(os.path.basename(sys.argv[0]))

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


def get_file_from_server(server, remote_fn, local_fn):
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
    cols = ["server", "fullname", "filename", "unk3"]
    q = """SELECT
            {}
           FROM ceph_server_files_old
           WHERE
            filename LIKE %s
        """.format(','.join(cols))
# FIXME: change unk3 to size when change to real table
    with PostgresDB() as db:
        for i, part in enumerate(parts_obj):
            # When in doubt, add more backslashes!
            rows = db.fetchall(q, ("{0}%".format(part["pattern"].replace('\\','\\\\')),))

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
    #print(parts_obj)

    parts_obj = find_parts_on_servers(parts_obj)
    #print(parts_obj)

    parts_obj = get_file_parts_from_servers(parts_obj)
    #print(parts_obj[0])

    r = reconstruct_file(parts_obj, stat_obj, output_dir)
    print(r)


#    print(get_file_from_server("c15node1",
#                               "/root/nbird",
#                               os.path.join(TMP_DIR, "asdf")))


#    rebuild_from_files(ceph_name, output_dir, files, stat_obj)
