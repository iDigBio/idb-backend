import os
import json
import traceback
import datetime

import psycopg2
from psycopg2.extras import RealDictCursor

from postgres_backend.db import PostgresDB
from riak import RiakClient
from helpers.etags import calcEtag

riak = RiakClient(nodes=[
    {"host": "c18node2.acis.ufl.edu"},
    {"host": "c18node6.acis.ufl.edu"},
    {"host": "c18node10.acis.ufl.edu"},
    {"host": "c18node12.acis.ufl.edu"},
    {"host": "c18node14.acis.ufl.edu"}
])

IDB_DBPASS = os.environ["IDB_DBPASS"]

db = PostgresDB()
local_cursor = db._cur

remote_pg = psycopg2.connect(host="c18node8.acis.ufl.edu",user="idigbio-api",password=IDB_DBPASS,dbname="idb-api-prod")
remote_cursor = remote_pg.cursor(cursor_factory=RealDictCursor)


# local_cursor.execute("select modified from uuids_data order by modified DESC limit 1")
# local_last_mod = local_cursor.fetchone()["modified"]

# print local_last_mod

# local_cursor.execute("select count(*) as count from uuids where deleted=true")
# local_delete_count = local_cursor.fetchone()["count"]

# print local_delete_count

# remote_cursor.execute("select modified from idigbio_uuids order by modified DESC limit 1")
# remote_last_mod = remote_cursor.fetchone()["modified"]

# print remote_last_mod

# remote_cursor.execute("select count(*) as count from idigbio_uuids where deleted=true")
# remote_delete_count = remote_cursor.fetchone()["count"]

# print remote_delete_count

def double_sorted_iterator(left,right, id_get=lambda x: x):
    lr = None
    rr = None
    while True:
        if rr is None:
            try:
                rr = next(right)
            except StopIteration:
                if lr is not None:
                    yield lr, None
                for lr in left:
                    yield lr, None
                break

        if lr is None:
            try:
                lr = next(left)
            except StopIteration:
                if rr is not None:
                    yield None, rr
                for rr in right:
                    yield None, rr
                break

        if id_get(lr) == id_get(rr):
            yield lr, rr
            lr = None
            rr = None
        elif id_get(lr) < id_get(rr):
            yield lr, None
            lr = None
        elif id_get(lr) > id_get(rr):
            yield none, rr
            rr = None

def get_riak_data(t,i,riak_etag):
    data_bucket = riak.bucket(t)
    do = data_bucket.get(i + "-" + riak_etag)
    data = None
    dm = None
    if do.data is not None:
        data = do.data["idigbio:data"]
        dm = do.data["idigbio:dateModified"]
    else:
        data = {}
        dm = datetime.datetime(2000,1,1,0,0,0)

    etag = calcEtag(data)
    return (etag,dm,data)

def update_record(r):
    try:
        data_list = []
        uuid_list = []
        version_list = []
        i = -1
        #print r
        bucket = riak.bucket(r["type"] + "_catalog")
        ro = bucket.get(r["id"])
        riak_etags = ro.data["idigbio:etags"]
        local_versions = db.get_item(r["id"],version="all")
        if len(local_versions) == 1 and local_versions[0]["etag"] is None:
            # There are no local versions, just an entry in the uuid table.
            pass
        else:
            try:
                for i,v in enumerate(local_versions):
                    if riak_etags[i] not in [v["riak_etag"], v["etag"]]:
                        (real_riak_etag,_,_) = get_riak_data(r["type"],r["id"],riak_etags[i])
                        if real_riak_etag not in [v["riak_etag"], v["etag"]]:
                            assert False
            except:
                traceback.print_exc()
                print "PROBLEM WITH HISTORY FOR", r["type"], r["id"], "SKIPPING"
                return ([],[],[])
        for j, riak_etag in enumerate(riak_etags[i+1:]):
            (etag,dm,data) = get_riak_data(r["type"],r["id"],riak_etag)
            data_list.append({
                "etag": etag,
                "riak_etag": riak_etag,
                "data": json.dumps(data)
            })
            version_list.append({
                "uuid": r["id"],
                "etag": etag,
                "version": i+j+1,
                "modified": dm
            })

        if len(local_versions) == 0:
            uuid_list.append({
                "uuid": r["id"],
                "type": r["type"],
                "parent": r["parent"] if "parent" in r else None,
                "deleted": r["deleted"]
            })

        return (data_list,uuid_list,version_list)
    except KeyboardInterrupt:
        raise KeyboardInterrupt
    except:
        print "Exception in record", r["id"]
        traceback.print_exc()
        return ([],[],[])

def run_transaction(data_list,uuid_list,version_list):
    print len(data_list), len(uuid_list), len(version_list)
    local_cursor.executemany("""INSERT INTO uuids (id,type,parent,deleted)
        SELECT %(uuid)s, %(type)s, %(parent)s, %(deleted)s WHERE NOT EXISTS (
            SELECT 1 FROM uuids WHERE id=%(uuid)s
        )
    """, uuid_list)
    local_cursor.executemany("""INSERT INTO data (etag,riak_etag,data)
        SELECT %(etag)s, %(riak_etag)s, %(data)s WHERE NOT EXISTS (
            SELECT 1 FROM data WHERE etag=%(etag)s
        )
    """, data_list)
    local_cursor.executemany("""INSERT INTO uuids_data (uuids_id,data_etag,version,modified)
        SELECT %(uuid)s, %(etag)s, %(version)s, %(modified)s WHERE NOT EXISTS (
            SELECT 1 FROM uuids_data WHERE uuids_id=%(uuid)s AND version=%(version)s
        )
    """, version_list)
    db.commit()    

def full_sync():
    uc = remote_pg.cursor("data sync get all", cursor_factory=RealDictCursor)
    uc.execute("select id,type,parent,version,etag,modified,deleted from idigbio_uuids order by id")

    local_ss_cursor = db._pg.cursor("data sync get all",cursor_factory=RealDictCursor)
    local_ss_cursor.execute("select uuid as id,type,parent,version,etag,modified,deleted from idigbio_uuids_new order by uuid")

    lr = None
    rr = None
    count = 0
    equal = 0
    different = {}
    for lr, rr in double_sorted_iterator(local_ss_cursor,uc,id_get=lambda x: x["id"]):
        count += 1
        if lr is None:
            different[rr["id"]] = rr
        elif rr is None:
            different[lr["id"]] = lr
        else:
            for f in ["type","etag","modified"]:
                if lr[f] != rr[f]:
                    different[rr["id"]] = rr
                    break
            else:
                equal += 1

        if count % 1000 == 0:
            print count, len(different), equal

    print count, len(different), equal
    data_list = []
    uuid_list = []
    version_list = []
    for r,d in different.iteritems():
        rdl, rul, rvl = update_record(d)
        data_list.extend(rdl)
        uuid_list.extend(rul)
        version_list.extend(rvl)

        if (len(data_list) + len(uuid_list) + len(version_list)) > 1000:            
            run_transaction(data_list,uuid_list,version_list)
            data_list = []
            uuid_list = []
            version_list = []

    run_transaction(data_list,uuid_list,version_list)        

def incremental_sync():
    local_cursor.execute("select modified from uuids_data order by modified DESC limit 1")
    local_last_mod = local_cursor.fetchone()["modified"]

    print local_last_mod

    remote_cursor.execute("select modified from idigbio_uuids order by modified DESC limit 1")
    remote_last_mod = remote_cursor.fetchone()["modified"]

    print remote_last_mod

    remote_cursor.execute("select count(*) as count from idigbio_uuids where modified > %s", (local_last_mod,))
    remote_mod_since_count = remote_cursor.fetchone()["count"]

    print remote_mod_since_count

    if remote_mod_since_count > 0:
        uc = remote_pg.cursor("data sync get modified", cursor_factory=RealDictCursor)
        uc.execute("select * from idigbio_uuids where modified > %s order by modified", (local_last_mod,))
        
        data_list = []
        uuid_list = []
        version_list = []
        for r in uc:
            rdl, rul, rvl = update_record(r)
            data_list.extend(rdl)
            uuid_list.extend(rul)
            version_list.extend(rvl)

            if (len(data_list) + len(uuid_list) + len(version_list)) > 1000:
                run_transaction(data_list,uuid_list,version_list)
                data_list = []
                uuid_list = []
                version_list = []

        run_transaction(data_list,uuid_list,version_list)

def sync_deletes():
    count = 0
    to_delete = set()
    to_undelete = set()

    dc = remote_pg.cursor("data sync get remote deletes", cursor_factory=RealDictCursor)
    dc.execute("select id from idigbio_uuids where deleted = true order by id")        

    ldc = db._pg.cursor("data sync get local deletes", cursor_factory=RealDictCursor)
    ldc.execute("select uuid as id from idigbio_uuids_new where deleted = true order by uuid")

    for lr, rr in double_sorted_iterator(ldc,dc,id_get=lambda x: x["id"]):
        count += 1
        if lr is None:
            to_delete.add((rr["id"],))
        elif rr is None:
            to_undelete.add((lr["id"],))
        else:
            pass

        if count % 1000 == 0:
            print count, len(to_delete), len(to_undelete)

    print count, len(to_delete), len(to_undelete)

    local_cursor.executemany("UPDATE uuids SET deleted=true WHERE id=%s", to_delete)
    db.commit()

    local_cursor.executemany("UPDATE uuids SET deleted=false WHERE id=%s", to_undelete)
    db.commit()

def main():
    import argparse

    parser = argparse.ArgumentParser(description='Synchronize the old and new database')
    parser.add_argument('-d','--deletes', dest='deletes', action='store_true', help='synchronize deletes')
    parser.add_argument('-i','--incremental', dest='incremental', action='store_true', help='run incremental sync')
    parser.add_argument('-f', '--full', dest='full', action='store_true', help='run full sync')

    args = parser.parse_args()

    if args.full:
        full_sync()

    if args.incremental:
        incremental_sync()

    if args.deletes:
        sync_deletes()

    if not (args.full or args.incremental or args.deletes):
        parser.print_help()

if __name__ == '__main__':
    main()