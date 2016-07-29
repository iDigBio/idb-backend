from gevent import monkey, pool
monkey.patch_all()

import os
import sys
import json
import itertools
import traceback
from collections import namedtuple, Counter

import re
pattern = re.compile('[\W_0-9]+|sp.')

import elasticsearch.helpers
from elasticsearch import Elasticsearch

from idb.helpers.etags import objectHasher

es = Elasticsearch([
    "c18node2.acis.ufl.edu",
    "c18node6.acis.ufl.edu",
    "c18node10.acis.ufl.edu",
    "c18node12.acis.ufl.edu",
    "c18node14.acis.ufl.edu"
], sniff_on_start=False, sniff_on_connection_fail=False, retry_on_timeout=True, max_retries=10, timeout=3)

# search_cache = {}
# if os.path.exists("search_cache.json"):
#     with open("search_cache.json", "rb") as pp:
#         search_cache = json.load(pp)
# print "Search Cache: {0}".format(len(search_cache))

good_status = {"accepted", "synonym"}
def run_query(q, cache_string):
    rsp = es.search(index="taxonnames",doc_type="taxonnames",body=q)


    best_response = None
    for h in rsp["hits"]["hits"]:
        if h["_source"]["dwc:scientificName"].lower() == cache_string:
            #search_cache[cache_string] = h["_source"]
            if h["_source"]["dwc:taxonomicStatus"] == "accepted":
                best_response = h
                break
            elif h["_source"]["dwc:taxonomicStatus"] == "synonym" and "dwc:acceptedNameUsageID" in h["_source"]:
                best_response = h
                break
        elif best_response is None:
            if h["_source"]["dwc:taxonomicStatus"] == "accepted":
                best_response = h
            elif h["_source"]["dwc:taxonomicStatus"] == "synonym" and "dwc:acceptedNameUsageID" in h["_source"]:
                best_response = h
        else:
            if h["_source"]["dwc:taxonomicStatus"] == "accepted":
                if h["_score"] > best_response["_score"]:
                    best_response = h
            elif h["_source"]["dwc:taxonomicStatus"] == "synonym" and "dwc:acceptedNameUsageID" in h["_source"]:
                if h["_score"] > best_response["_score"]:
                    best_response = h
            #print q, h

    #search_cache[cache_string] = None
    if best_response is not None:
        if best_response["_source"]["dwc:taxonomicStatus"] == "synonym":
            rsp = es.search(index="taxonnames",doc_type="taxonnames",body={
                "size": 1,
                "query": {
                    "term": {
                        "id": best_response["_source"]["dwc:acceptedNameUsageID"]
                    }
                }
            })
            if len(rsp["hits"]["hits"]) >= 1:
                return rsp["hits"]["hits"][0]["_source"]
            else:
                return None
        else:
            return best_response["_source"]
    else:
        return None

def fuzzy_wuzzy_string(s, rank="species"):
    # if s in search_cache:
    #     return search_cache[s]

    q = {
      "query": {
        "bool": {
          "must": [
            {
              "match": {
                "dwc:scientificName": {
                  "query": s,
                  "fuzziness": "AUTO"
                }
              }
            },
            { 
                "term": {
                    "dwc:taxonRank": rank
                }
            }
          ]
        }
      }
    }
    return run_query(q, s.lower())

stats = Counter()
def record_iterator():
    with open("taxon_etags.txt","rb") as inf:
        with open("taxon_matches.txt", "wb") as tm_file:
            for l in inf:
                stats["precount"] += 1
                etag, blob = l.strip().split("\t")
                r = json.loads(blob)
                yield (etag,r)

def result_collector(map_r):
    for r in map_r:
        stats[r[0]] += 1
        stats["postcount"] += 1

        if stats["postcount"] % 10000 == 0:
            print stats.most_common()

        yield r[1]

def work(t):
    etag, r = t
    try:
        rt = None
        match = None
        if "dwc:genus" in r and "dwc:specificepithet" in r:
            rt = { 
                "dwc:genus": r["dwc:genus"],
                "dwc:specificepithet": r["dwc:specificepithet"]
            }
            match = fuzzy_wuzzy_string(r["dwc:genus"] + " " + r["dwc:specificepithet"])
        elif "dwc:scientificName" in r:
            rt = {
                "dwc:scientificName": r["dwc:scientificName"]
            }
            match = fuzzy_wuzzy_string(r["dwc:scientificName"])
        elif "dwc:genus" in r:
            rt = {
                "dwc:genus": r["dwc:genus"]
            }
            match = fuzzy_wuzzy_string(r["dwc:genus"], rank="genus")
        else:
            print r
            return ("failout", (None, None))

        if match is not None:
            return ("match", (rt, match))
        else:
            return ("nomatch", (rt, { "flag_taxon_match_failed": True }))
    except KeyboardInterrupt:
        raise KeyboardInterrupt
    except:
        traceback.print_exc()
        return ("exception", (None, None))

def get_taxon_from_index():
    etags = set()

    t = "records"

    body = {
        "query": {
            "filtered": {
                "filter": {
                    "or": [
                        {
                            "exists": {
                                "field": "data.dwc:genus"
                            }
                        },
                        {
                            "exists": {
                                "field": "data.specificEpithet"
                            }
                        },
                        {
                            "exists": {
                                "field": "data.scientificName"
                            }
                        }
                    ]
                }
            }
        },
        "_source": [
            "data.dwc:genus",
            "data.dwc:specificEpithet",
            "data.dwc:scientificName",
        ]
    }

    for r in elasticsearch.helpers.scan(es, index="idigbio-2.9.3", query=body, size=1000, doc_type=t, scroll="10m"):
        etag = objectHasher("sha256", r["_source"], sort_arrays=True)    
        stats["count"] += 1
        if etag not in etags:
            stats["precount"] += 1
            etags.add(etag)
            yield (etag,r["_source"]["data"])        

def main():
    p = pool.Pool(25)

    with open("taxon_kv.txt", "wb") as outf:
        for r in result_collector(p.imap_unordered(work,get_taxon_from_index())):
            if r[0] is not None:
                outf.write(json.dumps(list(r)) + "\n")

    print stats.most_common()

    # with open("search_cache.json", "wb") as pp:
    #     json.dump(search_cache,pp)

if __name__ == '__main__':
    main()