import elasticsearch

es = elasticsearch.Elasticsearch([
    "c17node52.acis.ufl.edu:9200",
    "c17node53.acis.ufl.edu:9200",
    "c17node54.acis.ufl.edu:9200",
    "c17node55.acis.ufl.edu:9200",
    "c17node56.acis.ufl.edu:9200"
], timeout=3000)

query = {"size": 0, "query": {"and": [{"exists": {"field": "geopoint"}},{"exists":{"field":"taxonid"}},{"exists":{"field":"canonicalname"}}]}, "aggs": {"tt": {"terms": {
    "field": "taxonid", "size": 200000, "min_doc_count": 20}, "aggs": {"cn": {"terms": {"field": "canonicalname"}}}}}}


rsp = es.search(index="idigbio-3.0.0", doc_type="records", body=query)

for k in rsp:
    if k not in ["aggregations"]:
        print k, rsp[k]
for k in rsp["aggregations"]["tt"]:
    if k not in ["buckets"]:
        print k, rsp["aggregations"]["tt"][k]        
with open("taxon_ids.txt","wb") as outf:
    for b in rsp["aggregations"]["tt"]["buckets"]:
        outf.write("{} {} {}".format(b["key"], b["doc_count"], b["cn"]["buckets"][0]["key"].encode("utf-8")) + "\n")