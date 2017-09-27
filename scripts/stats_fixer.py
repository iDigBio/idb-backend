from elasticsearch import Elasticsearch
import elasticsearch.helpers
import os.path
import json

doit = True

def average(l):
    return int(sum(l) / float(len(l)))


es = Elasticsearch([
                "c18node2.acis.ufl.edu:9200",
                "c18node6.acis.ufl.edu:9200",
                "c18node10.acis.ufl.edu:9200",
                "c18node12.acis.ufl.edu:9200",
                "c18node14.acis.ufl.edu:9200"
], sniff_on_start=False, sniff_on_connection_fail=False,timeout=30)

stats_query = {
    "query": {
        "bool": {
            "must": [
                {
                    "range": {
                        "harvest_date": {
                            "gte": "2017-02-01",
                            "lte": "2017-04-30"
                        }
                    }
                }
            ],
            "must_not": [],
            "should": []
        }
    }
}

if os.path.exists("days.json"):
    with open("days.json", "rb") as inf:
        days = json.load(inf)
else:
    count = 0
    days = {"{0:02}".format(d): {} for d in range(1,32)}
    purge = set()
    for r in elasticsearch.helpers.scan(es,**{
        "index": "stats",
        "doc_type": "search",
        "size": 500,
        "scroll": "5m",
        "query": stats_query
    }):
        rsid = r["_source"]["recordset_id"]
        day = r["_source"]["harvest_date"][8:]
        month = r["_source"]["harvest_date"][5:7]
        if month == "03":
            purge.add(r["_id"])
            continue

        if rsid in days[day]:
            days[day][rsid][month] = r["_source"]
        else:
            days[day][rsid] = {month: r["_source"]}
        count += 1

    print(count)
    with open("days.json","wb") as outf:
        json.dump(days, outf)

    if doit:
        for p in purge:
            es.delete(index="stats", doc_type="search", id=p)
    else:
        print(purge)


march = {"{0:02}".format(d): {} for d in range(1,31)}
types = ["records", "mediarecords"]
cats = ["download", "seen", "search", "mapping", "view"]

for d in march:
    for rsid in days[d]:
        new_data = {t: {c: {"count": [], "total": []} for c in cats} for t in types}
        for m, s in days[d][rsid].items():

            for t in types:
                for c in cats:
                    new_data[t][c]["count"].append(s[t][c]["count"])
                    new_data[t][c]["total"].append(s[t][c]["total"])

        for t in types:
            for c in cats:
                new_data[t][c]["count"] = average(new_data[t][c]["count"])
                new_data[t][c]["total"] = average(new_data[t][c]["total"])
        march[d][rsid] = new_data

for d in march:
    for rsid in march[d]:
        march[d][rsid]["harvest_date"] = "2017-03-" + d
        march[d][rsid]["recordset_id"] = rsid

        if doit:
            es.create(index="stats", doc_type="search", body=march[d][rsid])
        else:
            # print(march[d][rsid])
            pass
