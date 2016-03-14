from __future__ import absolute_import
import json
import dateutil.parser
import elasticsearch

from idb.config import config
from idb.postgres_backend import apidbpool, DictCursor
from idb.postgres_backend.stats_db import statsdbpool

from collections import defaultdict
from datetime import datetime, timedelta


def json_serial(obj):
    """JSON serializer for objects not serializable by default json code"""

    if isinstance(obj, datetime):
        serial = obj.isoformat()
        return serial
    raise TypeError ("Type not serializable")

indexName = "stats-2.5.0"
typeName = "search"

sl = config["elasticsearch"]["servers"]
# sl = [
#     "c17node52.acis.ufl.edu",
#     "c17node53.acis.ufl.edu",
#     "c17node54.acis.ufl.edu",
#     "c17node55.acis.ufl.edu",
#     "c17node56.acis.ufl.edu"
# ]

es = elasticsearch.Elasticsearch(sl, retry_on_timeout=True,max_retries=10,timeout=30)

record_types = ["records","mediarecords"]
stat_types = ["download","mapping","search","seen","view"]

search_stats_mapping = {
    "properties": {
        "recordset_id": { "type" : "string", "analyzer": "keyword" },
        "harvest_date": { "type": "date" }
    }
}
for record_type in record_types:
    search_stats_mapping["properties"][record_type] = {
        "properties": {}
    }
    for stat_type in stat_types:
        search_stats_mapping["properties"][record_type]["properties"][stat_type] = {
            "properties": {
                "count": { "type" : "double" },
                "total": { "type" : "double" },
                "items": {
                    "properties": {
                        "count": { "type": "double" },
                        "term": { "type" : "string", "analyzer": "keyword" }
                    }
                },
                "queries": {
                    "properties": {
                        "count": { "type": "double" },
                        "term": { "type" : "string", "analyzer": "keyword" }
                    }
                },
                "geocodes": {
                    "properties": {
                        "count": { "type": "double" },
                        "geo": {
                            "properties": {
                                "country": { "type" : "string", "analyzer": "keyword" },
                                "region": { "type" : "string", "analyzer": "keyword" },
                                "city": { "type" : "string", "analyzer": "keyword" },
                            }
                        }
                    }
                }
            }
        }

def new_stats_dict():
    stats_dict = {}
    for record_type in record_types:
        stats_dict[record_type] = {}
        for stat_type in stat_types:
            stats_dict[record_type][stat_type] = {
                "count": 0,
                "total": 0
            }
            stats_dict[record_type][stat_type]["items"] = defaultdict(int)
            stats_dict[record_type][stat_type]["queries"] = defaultdict(int)
            stats_dict[record_type][stat_type]["geocodes"] = defaultdict(int)
    return stats_dict

def get_stats_dates():
    sql = """select date_trunc('day', date)
    		from stats
    		group by date_trunc('day', date)
    		order by date_trunc('day', date)"""
    return [r[0] for r in statsdbpool.fetchall(sql)]

def collect_stats(collect_datetime):
    date_min = (collect_datetime - timedelta(1)).date()
    date_max = collect_datetime.date()

    recordset_stats = defaultdict(new_stats_dict)

    #print date_min, date_max
    sql = "SELECT * FROM stats LEFT JOIN queries on stats.query_id=queries.id WHERE date > %s AND date < %s"
    for r in statsdbpool.fetchiter(sql, (date_min, date_max), cursor_factory=DictCursor):
        record_type = r["record_type"]
        stats_type = r["type"]
        query_hash = r["query_hash"]
        geocode = json.dumps(r["ip_geocode"],sort_keys=True)
        if record_type in record_types:
            if stats_type == "view":
                for record_key in r["payload"]:
                    recordset_key = r["payload"][record_key]
                    recordset_stats[recordset_key][record_type][stats_type]["count"] += 1
                    recordset_stats[recordset_key][record_type][stats_type]["total"] += 1
                    recordset_stats[recordset_key][record_type][stats_type]["items"][record_key] += 1
                    recordset_stats[recordset_key][record_type][stats_type]["geocodes"][geocode] += 1
            elif stats_type == "seen":
                for record_key in r["payload"]:
                    recordset_key = r["payload"][record_key]
                    recordset_stats[recordset_key][record_type][stats_type]["count"] += 1
                    recordset_stats[recordset_key][record_type][stats_type]["total"] += 1
                    recordset_stats[recordset_key][record_type][stats_type]["items"][record_key] += 1
                    recordset_stats[recordset_key][record_type][stats_type]["queries"][query_hash] += 1
                    recordset_stats[recordset_key][record_type][stats_type]["geocodes"][geocode] += 1
            else:
                for recordset_key in r["payload"]:
                    record_count = r["payload"][recordset_key]
                    recordset_stats[recordset_key][record_type][stats_type]["count"] += 1
                    recordset_stats[recordset_key][record_type][stats_type]["total"] += record_count
                    recordset_stats[recordset_key][record_type][stats_type]["queries"][query_hash] += record_count
                    recordset_stats[recordset_key][record_type][stats_type]["geocodes"][geocode] += record_count

    for recordset_key in recordset_stats:
        recordset_data = {
            "recordset_id": recordset_key,
            "harvest_date": collect_datetime.date().isoformat()
        }
        for record_type in record_types:
            recordset_data[record_type] = {}
            for stat_type in stat_types:
                recordset_data[record_type][stat_type] = {
                    "count": recordset_stats[recordset_key][record_type][stat_type]["count"],
                    "total": recordset_stats[recordset_key][record_type][stat_type]["total"],
                    "items": [],
                    "queries": [],
                    "geocodes": []
                }
                for k in recordset_stats[recordset_key][record_type][stat_type]["items"]:
                    recordset_data[record_type][stat_type]["items"].append({
                        "term": k,
                        "count": recordset_stats[recordset_key][record_type][stat_type]["items"][k]
                    })
                for k in recordset_stats[recordset_key][record_type][stat_type]["queries"]:
                    recordset_data[record_type][stat_type]["queries"].append({
                        "term": k,
                        "count": recordset_stats[recordset_key][record_type][stat_type]["queries"][k]
                    })
                for k in recordset_stats[recordset_key][record_type][stat_type]["geocodes"]:
                    recordset_data[record_type][stat_type]["geocodes"].append({
                        "geo": json.loads(k),
                        "count": recordset_stats[recordset_key][record_type][stat_type]["geocodes"][k]
                    })

        es.index(index=indexName,doc_type=typeName,body=recordset_data)

def api_stats():
    now = datetime.utcnow().isoformat()

    rstc = defaultdict(lambda: defaultdict(int))

    sql = """SELECT parent,type,count(id)
             FROM uuids
             WHERE deleted=false and (type='record' or type='mediarecord')
             GROUP BY parent,type"""
    for r in apidbpool.fetchiter(sql):
        rstc[r[0]][r[1]+"s_count"]=r[2]

    rstc=dict(rstc)

    for k in rstc:
        rsc = rstc[k]
        if "records_count" not in rsc:
            rsc["records_count"] = 0
        if "mediarecords_count" not in rsc:
            rsc["mediarecords_count"] = 0
        rsc["harvest_date"] = now
        rsc["recordset_id"] = k

        es.index(index=indexName,doc_type="api",body=rsc)

def main():
    import argparse

    parser = argparse.ArgumentParser(description='Collect the stats for a day from postgres, defaults to yesterday')
    parser.add_argument('-d', '--date', dest='collect_date_str', type=str, default=datetime.now().isoformat())
    parser.add_argument('-m', '--mapping', dest='mapping', action='store_true', help='write mapping')
    parser.add_argument('-a', '--alldates', dest='alldates', action='store_true', help='write stats for all dates in the db')
    parser.add_argument('-p', '--api', dest='api', action='store_true', help="write out the api stats")

    args = parser.parse_args()

    if args.mapping:
        es.indices.put_mapping(index=indexName,doc_type=typeName,body={ typeName: search_stats_mapping })

    if args.api:
        api_stats()
    else:
        if args.alldates:
            for d in get_stats_dates():
                print "Running stats for", d
                collect_stats(d)
        else:
            collect_datetime = dateutil.parser.parse(args.collect_date_str)
            collect_stats(collect_datetime)

if __name__ == '__main__':
    main()
