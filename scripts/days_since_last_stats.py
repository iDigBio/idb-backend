from elasticsearch import Elasticsearch
from datetime import timedelta, datetime

t = datetime.now()
last30 = t - timedelta(days=30)


es = Elasticsearch([
                "c20node1.acis.ufl.edu:9200",
                "c20node2.acis.ufl.edu:9200",
                "c20node3.acis.ufl.edu:9200",
                "c20node4.acis.ufl.edu:9200",
                "c20node5.acis.ufl.edu:9200",
                "c20node6.acis.ufl.edu:9200",
                "c20node7.acis.ufl.edu:9200",
                "c20node8.acis.ufl.edu:9200",
                "c20node9.acis.ufl.edu:9200",
                "c20node10.acis.ufl.edu:9200",
                "c20node11.acis.ufl.edu:9200",
                "c20node12.acis.ufl.edu:9200"
], sniff_on_start=False, sniff_on_connection_fail=False,timeout=30)

stats_query = {
    "query": {
        "bool": {
            "must": [
                {
                    "range": {
                        "harvest_date": {
                            "gte": last30.isoformat(),
                            "lte": t.isoformat()
                        }
                    }
                }
            ],
            "must_not": [],
            "should": []
        }
    },
    "size": 0,
    "aggs": {
        "dh": {
            "date_histogram": {
                "field": "harvest_date",
                "interval": "day"
            }
        }
    }
}

rv = es.search(**{
        "index": "stats",
        "doc_type": "search",
        "body": stats_query
})

min_days = 31

for b in rv["aggregations"]["dh"]["buckets"]:
    min_days = min(min_days, (t - datetime.strptime(b["key_as_string"], "%Y-%m-%dT%H:%M:%S.%fZ")).days)

print(min_days)