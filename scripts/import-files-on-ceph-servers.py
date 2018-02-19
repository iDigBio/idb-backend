import os
import sys
from idb.helpers.logging import getLogger, configure_app_log
logger = getLogger("restore")
from idb.postgres_backend.db import PostgresDB

# When Ceph chewed up a bunch of objects (ticket #2605), we figured out how to
# rebuild them by searching for the file parts on disk. We generated lists of
# files on all ceph nodes with:
#  find /srv/ceph -ls > <text file>
# which we then import below into a postgres table in idb-api-beta for rapid
# look ups and searching.


# Table was created manually with below:
#\connect idb_api_beta
#CREATE TABLE ceph_server_files (
#    server VARCHAR(16) NOT NULL,
#    line INTEGER,
#    unk INTEGER,
#    perms VARCHAR(16),
#    unk2 INTEGER,
#    owner_name VARCHAR(16),
#    group_name VARCHAR(16),
#    unk3 INTEGER,
#    day INTEGER,
#    month VARCHAR(3),
#    year_time VARCHAR(8),
#    fullname TEXT NOT NULL,
#    filename TEXT NOT NULL
#);
#
#alter table ceph_server_files owner to idigbio
#
#create index index_ceph_on_filename_with_pattern_ops
#on ceph_server_files (filename text_pattern_ops);
#CREATE UNIQUE INDEX index_ceph_fullname
#ON ceph_server_files (fullname);



def file_list_iter(fn):
    with open(fn, 'r') as f:
        for l in f:
           fields = l.split()
           if "current" in fields[11]:
               fields[0] = fields[0][:-1] # trim ":" from end of server name
               fields[11] = fields[11].replace("\\\\", "\\") # de-escape slashes produced by `find -ls` to get the real file name
               fields.append(os.path.basename(fields[11])) # only fn for prefix searching
               yield fields

if __name__ == '__main__':

    fn = sys.argv[1]

    with PostgresDB() as db:
        q = """INSERT INTO ceph_server_files
               (server, line, unk, perms, unk2, owner_name, group_name, 
                unk3, month, day, year_time, fullname, filename)
               VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
               ON CONFLICT DO NOTHING
            """
        c = 1
        for l in file_list_iter(fn):
            db.execute(q, l)
            c += 1
            if (c % 100000) == 0:
                db.commit()
                c = 1
#                break

        db.commit()

#        rows = db.fetchall("SELECT * FROM ceph_server_files LIMIT 3")
#        for f in rows:
#            print(f)

