import requests
import json
import unicodecsv as csv

search_server = "http://c18node2.acis.ufl.edu:9200"
index = "idigbio-2.4.0"
pattern = "{0}/{1}/{2}/_search"

s = requests.Session()

pub_query = {
    "size": 1000,
    "_source": ["data.name"]
}

rp = s.post(pattern.format(search_server,index,"publishers"), data=json.dumps(pub_query))
rp.raise_for_status()
op = rp.json()

pubs = {}

for h in op["hits"]["hits"]:
    try:
        pubs[h["_id"]] = h["_source"]["data"]["name"]
    except:
        print "skip pub", h["_id"]

rs_query = {
    "size": 1000,
    "_source": ["publisher","data.collection_name"]
}

rs = s.post(pattern.format(search_server,index,"recordsets"), data=json.dumps(rs_query))
rs.raise_for_status()
os = rs.json()

rsp = {}

for h in os["hits"]["hits"]:
    try:
        rsp[h["_id"]] = { "pub": h["_source"]["publisher"], "pub_name": pubs[h["_source"]["publisher"]], "name": h["_source"]["data"]["collection_name"]}
    except:
        print "skip rs", h["_id"]

query = {
    "aggs": {
        "rs": {
            "terms": {
                "field": "recordset",
                "size": 500
            },
            "aggs": {
                "kv": {
                    "terms": {
                        "field": "flags",
                        "size": 200
                    }
                },
                "dqs": {
                    "stats": {
                        "field": "dqs"
                    }
                }
            }
        },
        "kv": {
            "terms": {
                "field": "flags",
                "size": 200
            }
        },
        "dqs": {
            "stats": {
                "field": "dqs"
            }
        }
    },
    "size": 0
}

r = s.post(pattern.format(search_server,index,"records"),data=json.dumps(query))
r.raise_for_status()
o = r.json()

flag_names = []
global_flag_vals = []
global_stats = o["aggregations"]["dqs"]
for f in o["aggregations"]["kv"]["buckets"]:
    flag_names.append(f["key"])
    global_flag_vals.append(f["doc_count"])

with open("dq_flags.csv","wb") as outf:
    cw = csv.writer(outf)
    cw.writerow(["recordset_id", "recordset_name", "publisher_id", "publisher_name", "rs_count", "max_dqs", "min_dqs", "average_dqs"] + flag_names)
    cw.writerow(["","all idigbio","","idigbio",o["hits"]["total"],global_stats["max"],global_stats["min"],global_stats["avg"]] + global_flag_vals)
    for rs in o["aggregations"]["rs"]["buckets"]:
        flag_vals = [0 for _ in flag_names]
        stats = rs["dqs"]
        for f in rs["kv"]["buckets"]:
            flag_vals[flag_names.index(f["key"])] = f["doc_count"]
        cw.writerow([rs["key"],rsp[rs["key"]]["name"],rsp[rs["key"]]["pub"],rsp[rs["key"]]["pub_name"],rs["doc_count"],stats["max"],stats["min"],stats["avg"]] + flag_vals)