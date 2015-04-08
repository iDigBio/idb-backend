import os
import sys
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

def output_line(*l):
    sys.stdout.write(" ".join([repr(p) for p in l]))

def restart_line():
    sys.stdout.write('\r')
    sys.stdout.flush()

def end_line():
    sys.stdout.write('\r\n')
    sys.stdout.flush()

def double_sorted_iterator(left,right, id_get=lambda x: x):
    lr = None
    rr = None
    nlr = None
    nrr = None
    try:
        while True:
            if rr is None:
                if nrr is None:
                    rr = next(right)
                else:
                    rr = nrr
                    nrr = None

            if lr is None:
                if nlr is None:
                    lr = next(left)
                else:
                    lr = nlr
                    nlr = None

            if id_get(lr) == id_get(rr):
                yield lr, rr
                lr = None
                rr = None
            elif id_get(lr) < id_get(rr):
                nlr = next(left)
                # -1 to 1
                if id_get(nlr) > id_get(rr):
                    yield None, rr
                    rr = None
                else:
                    yield lr, None
                    lr = None
            elif id_get(lr) > id_get(rr):
                nrr = next(right)

                # 1 to -1
                if id_get(lr) < id_get(nrr):
                    yield lr, None
                    lr = None
                else:
                    yield None, rr
                    rr = None
    except StopIteration:
        if lr is not None:
            yield lr, None
            for lr in left:
                yield lr, None
        if rr is not None:
            yield None, rr
            for rr in right:
                yield None, rr

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
    print "Running Full Sync"
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
            output_line(count, len(different), equal)
            restart_line()

    output_line(count, len(different), equal)
    end_line()
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
    print "Running Incemental Sync"
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
    print "Syncing Deletes"
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
            output_line(count, len(to_delete), len(to_undelete))
            restart_line()

    output_line(count, len(to_delete), len(to_undelete))
    end_line()

    local_cursor.executemany("UPDATE uuids SET deleted=true WHERE id=%s", to_delete)
    db.commit()

    local_cursor.executemany("UPDATE uuids SET deleted=false WHERE id=%s", to_undelete)
    db.commit()

def load_null_data():
    print "Fixing up null records"
    ldc = db._pg.cursor("data sync get local null data", cursor_factory=RealDictCursor)
    ldc.execute("select uuid,type,etag,riak_etag from idigbio_uuids_data where data is null")

    items = []
    for r in ldc:
        items.append([r["type"],r["uuid"],r["riak_etag"],r["etag"]])

    print len(items)
    data = []
    count = 0
    for r in items:
        count += 1
        (_,_,riak_data) = get_riak_data(*r[0:-1])
        data.append({"etag": r[-1],"data": json.dumps(riak_data)})

        if len(data) >= 1000:
            print count, len(data)
            local_cursor.executemany("UPDATE data SET data=%(data)s WHERE etag=%(etag)s", data)
            db.commit()
            data = []

    print count, len(data)
    local_cursor.executemany("UPDATE data SET data=%(data)s WHERE etag=%(etag)s", data)
    db.commit()

def sync_identifiers():
    print "Syncing RecordIds"
    # Note: Postgresql order by for text columns is inconsistent with python lexigraphcal ordering of unicode strings
    # This function uses slightly more complicated logic to achieve memory efficiency
    count = 0
    to_insert = set()
    missing = set()

    dc = remote_pg.cursor("data sync get remote ids", cursor_factory=RealDictCursor)
    dc.execute("select idigbio_id_list.id as uuid, array_agg(provider_id) as ids from idigbio_id_list join idigbio_uuids on idigbio_id_list.id=idigbio_uuids.id group by idigbio_id_list.id order by idigbio_id_list.id")

    ldc = db._pg.cursor("data sync get local ids", cursor_factory=RealDictCursor)
    ldc.execute("select array_agg(identifier) as ids,uuids_id as uuid from uuids_identifier group by uuids_id order by uuids_id")

    for lr, rr in double_sorted_iterator(ldc,dc,id_get=lambda x: x["uuid"]):
        count += 1
        if lr is None:
            for i in rr["ids"]:
                to_insert.add((i,rr["uuid"]))
        elif rr is None:
            for i in lr["ids"]:
                missing.add((i,lr["uuid"]))
        else:
            lset = set(lr["ids"])
            rset = set(rr["ids"])
            for i in lset - rset:
                missing.add((i,lr["uuid"]))
            for i in rset - lset:
                to_insert.add((i,rr["uuid"]))

        if count % 1000 == 0:
            output_line(count, len(to_insert), len(missing))
            restart_line()

    output_line(count, len(to_insert), len(missing))
    end_line()

    local_cursor.executemany("INSERT INTO uuids_identifier (identifier,uuids_id) VALUES (%s,%s)", to_insert)
    db.commit()

def sync_siblings():
    print "Syncing Siblings"
    # Note: Postgresql order by for text columns is inconsistent with python lexigraphcal ordering of unicode strings
    # This function uses slightly more complicated logic to achieve memory efficiency
    count = 0
    to_insert = set()
    missing = set()

    dc = remote_pg.cursor("data sync get remote siblings", cursor_factory=RealDictCursor)
    dc.execute("""SELECT
            subject,
            object
        FROM (
            SELECT subject,object
            FROM idigbio_relations
            UNION
            SELECT object,subject
            FROM idigbio_relations
        ) as a
        JOIN idigbio_uuids as b
        ON subject=b.id
        JOIN idigbio_uuids as c
        ON object=c.id
        WHERE subject < object
        ORDER BY subject, object
    """)

    ldc = db._pg.cursor("data sync get local siblings", cursor_factory=RealDictCursor)
    ldc.execute("""SELECT subject, object FROM (
            SELECT r1 as subject,r2 as object
            FROM uuids_siblings
            UNION
            SELECT r2 as subject,r1 as object
            FROM uuids_siblings
        ) AS a
        WHERE subject < object
        ORDER BY subject, object
    """)

    for lr, rr in double_sorted_iterator(ldc,dc,id_get=lambda x: (x["subject"],x["object"])):
        count += 1
        if lr is None:
            to_insert.add((rr["subject"],rr["object"]))
        elif rr is None:
            missing.add((lr["subject"],lr["object"]))
        else:
            pass

        if count % 1000 == 0:
            output_line(count, len(to_insert), len(missing))
            restart_line()

    output_line(count, len(to_insert), len(missing))
    end_line()

    local_cursor.executemany("INSERT INTO uuids_siblings (r1,r2) VALUES (%s,%s)", to_insert)
    db.commit()

def main():
    import argparse

    parser = argparse.ArgumentParser(description='Synchronize the old and new database')
    parser.add_argument('-d','--deletes', dest='deletes', action='store_true', help='synchronize deletes')
    parser.add_argument('-i','--incremental', dest='incremental', action='store_true', help='run incremental sync')
    parser.add_argument('-f', '--full', dest='full', action='store_true', help='run full sync')
    parser.add_argument('-n', '--fixnull', dest='fixnull', action='store_true', help='fix null current data')
    parser.add_argument('-r', '--recordids', dest='recordids', action='store_true', help='fix null current data')
    parser.add_argument('-s', '--siblings', dest='siblings', action='store_true', help='fix null current data')

    args = parser.parse_args()

    if args.full:
        full_sync()
        sync_deletes()
        sync_identifiers()
        sync_siblings()

    if args.incremental:
        incremental_sync()

    if args.deletes:
        sync_deletes()

    if args.fixnull:
        load_null_data()

    if args.recordids:
        sync_identifiers()

    if args.siblings:
        sync_siblings()

    if not any(args.__dict__.values()):
        parser.print_help()

if __name__ == '__main__':
    main()