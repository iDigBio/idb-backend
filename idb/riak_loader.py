from data_sync import get_riak_data, db
import json
import sys

def task(partition):
    count = 1
    
    total_count = 0
    while count > 0:
        db._cur.execute("select uuids.type, uuids_data.uuids_id, riak_etag from data join uuids_data on data.etag=uuids_data.data_etag join uuids on uuids.id = uuids_data.uuids_id where data is null and riak_etag like %s LIMIT 10000", (partition + "%",))
        data_items = []
        for r in db._cur:
            t, u, riak_etag = r
            
            etag, dm, data = get_riak_data(t, u, riak_etag)
            data_items.append((json.dumps(data),riak_etag))

        db._cur.executemany("UPDATE data SET data=%s WHERE riak_etag=%s", data_items)
        db.commit()
        total_count += len(data_items)
        print partition, total_count

def main():
    # #p = Pool(16)
    # #p.map(task,[hex(i)[-1] for i in range(0,16)])
    # map(task,[hex(i)[-1] for i in range(0,16)])
    task(sys.argv[1])


if __name__ == '__main__':
    main()