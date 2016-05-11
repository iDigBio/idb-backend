from __future__ import division, absolute_import, print_function
import sys

from idb.postgres_backend import apidbpool
from idb.stats_collector import es, indexName
import datetime


def post_delete_stats(rsid):
    summary = {
        "recordset_id": rsid,
        "harvest_date": datetime.datetime.now().isoformat(),
        "records_count": 0,
        "records_create": 0,
        "records_update": 0,
        "records_delete": 0,
        "mediarecords_count": 0,
        "mediarecords_create": 0,
        "mediarecords_update": 0,
        "mediarecords_delete": 0,
        "deleted": True,
        "commited": True,
    }
    es.index(index=indexName, doc_type="digest", body=summary)


def post_all_deleted_rs():
    deleted_query = {
        "query": {
            "filtered": {
                "filter": {
                    "term": {
                        "deleted": True
                    }
                }
            }
        },
        "size": 10000
    }

    deleted_recordsets = set()

    rsp = es.search(index=indexName, doc_type="digest", body=deleted_query)
    for rs in rsp["hits"]["hits"]:
        deleted_recordsets.add(rs["_source"]["recordset_id"])

    print("{} recordsets already marked as deleted in stats.".format(
        len(deleted_recordsets)))

    count = 0
    with apidbpool.cursor() as cursor:
        cursor.execute("SELECT id FROM uuids WHERE type='recordset' and deleted=true")

        for r in cursor:
            if r["id"] not in deleted_recordsets:
                count += 1
                print("Deleting {}.".format(r["id"]))
                post_delete_stats(r["id"])

    print("{} recordsets deleted from stats.".format(count))


def main():
    if len(sys.argv) > 1:
        post_delete_stats(sys.argv[1])
    else:
        post_all_deleted_rs()

if __name__ == '__main__':
    main()
