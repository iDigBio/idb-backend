import os
import sys

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
    es.index(index=indexName,doc_type="digest",body=summary)

def main():
    post_delete_stats(sys.argv[1])

if __name__ == '__main__':
    main()