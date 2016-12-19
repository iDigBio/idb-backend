from gevent import monkey, pool
monkey.patch_all()

import sys
import json
import traceback
from collections import Counter

import re
pattern = re.compile('[\W_0-9]+|sp.')

import elasticsearch.helpers
from elasticsearch import Elasticsearch

from idb.helpers.etags import objectHasher
import taxon_rank

DEBUG = False

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
score_stats = Counter()
def run_query(q, cache_string, log_score=True):
    rsp = es.search(index="taxonnames",doc_type="taxonnames",body=q)

    best_response = None
    for h in rsp["hits"]["hits"]:
        # if DEBUG:
        #     print(h)
        if h["_source"]["dwc:scientificName"].lower() == cache_string:
            #search_cache[cache_string] = h["_source"]
            if h["_source"]["dwc:taxonomicStatus"] == "accepted":
                best_response = h
                break
            elif "synonym" in h["_source"]["dwc:taxonomicStatus"] and "dwc:acceptedNameUsageID" in h["_source"]:
                best_response = h
                break
        elif best_response is None:
            #print (h["_score"], h["_source"]["dwc:taxonomicStatus"])
            if h["_source"]["dwc:taxonomicStatus"] == "accepted":
                best_response = h
            elif "synonym" in h["_source"]["dwc:taxonomicStatus"] and "dwc:acceptedNameUsageID" in h["_source"]:
                best_response = h
        else:
            #print(h["_score"], h["_source"]["dwc:taxonomicStatus"], best_response["_score"], h["_score"] > best_response["_score"])
            if h["_source"]["dwc:taxonomicStatus"] == "accepted":
                if h["_score"] > best_response["_score"]:
                    best_response = h
            elif "synonym" in h["_source"]["dwc:taxonomicStatus"] and "dwc:acceptedNameUsageID" in h["_source"]:
                if h["_score"] > best_response["_score"]:
                    best_response = h
            #print q, h

    #search_cache[cache_string] = None
    if best_response is not None:
        if log_score:
            score_stats[int(best_response["_score"]*10)] += 1

        # Reject low quality matches
        if best_response["_score"] <= 1.0:
            return None

        if DEBUG:
            print(best_response, "synonym" in best_response["_source"]["dwc:taxonomicStatus"])
        if "synonym" in best_response["_source"]["dwc:taxonomicStatus"]:
            rsp = es.search(index="taxonnames",doc_type="taxonnames",body={
                "size": 1,
                "query": {
                    "term": {
                        "dwc:taxonID": best_response["_source"]["dwc:acceptedNameUsageID"]
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

def fuzzy_wuzzy_string_new(s, rank="species", should=None):
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
                },
                {
                    "or": [
                        {
                            "term": {
                                "dwc:kingdom": "animalia"
                            }
                        },
                        {
                            "term": {
                                "dwc:kingdom": "plantae"
                            }
                        },
                        {
                            "term": {
                                "dwc:kingdom": "fungi"
                            }
                        },
                        {
                            "term": {
                                "dwc:kingdom": "chromista"
                            }
                        },
                        {
                            "term": {
                                "dwc:kingdom": "protista"
                            }
                        },
                        {
                            "term": {
                                "dwc:kingdom": "protozoa"
                            }
                        }
                    ]
                }
            ]
        }
      }
    }

    if should is not None:
        q["query"]["bool"]["should"] = []
        shd = q["query"]["bool"]["should"]
        for k in should:
            shd.append({
                "match": {
                    k: {
                        "query": should[k],
                        "fuzziness": "AUTO"
                    }
                }
            })

    # if DEBUG:
    #     print(json.dumps(q, indent=2))

    return run_query(q, s.lower())

def fuzzy_wuzzy_string(s, rank="species", should=None):
    # if s in search_cache:
    #     return search_cache[s]

    q = {
      "query": {
        "bool": {
            "must": [
                # {
                #   "match": {
                #     "dwc:scientificName": {
                #       "query": s,
                #       "fuzziness": "AUTO"
                #     }
                #   }
                # },
                {
                    "term": {
                        "dwc:taxonRank": rank
                    }
                },
                {
                    "or": [
                        {
                            "term": {
                                "dwc:kingdom": "animalia"
                            }
                        },
                        {
                            "term": {
                                "dwc:kingdom": "plantae"
                            }
                        },
                        {
                            "term": {
                                "dwc:kingdom": "fungi"
                            }
                        },
                        {
                            "term": {
                                "dwc:kingdom": "chromista"
                            }
                        },
                        {
                            "term": {
                                "dwc:kingdom": "protista"
                            }
                        },
                        {
                            "term": {
                                "dwc:kingdom": "protozoa"
                            }
                        }
                    ]
                }
            ]
        }
      }
    }

    if should is not None:
        q["query"]["bool"]["should"] = []
        shd = q["query"]["bool"]["should"]
        for k in should:
            shd.append({
                "match": {
                    k: {
                        "query": should[k],
                        "fuzziness": "AUTO"
                    }
                }
            })

    # if DEBUG:
    #     print(json.dumps(q, indent=2))

    return run_query(q, s.lower(), log_score=False)


stats = Counter()
def record_iterator():
    with open("taxon_etags.txt","rb") as inf:
        #with open("taxon_matches.txt", "wb") as tm_file:
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
            print(stats.most_common(), score_stats.most_common())
            show_plot()

        yield r[1]

def work(t):
    etag, r = t
    try:
        rt = None
        match = None
        match_new = None

        should = {}

        for k in ["dwc:kingdom", "dwc:phylum", "dwc:class", "dwc:order", "dwc:family", "dwc:genus", "dwc:specificEpithet", "dwc:scientificName"]:
            if k in r:
                should[k] = r[k]

        if "dwc:genus" in r and "dwc:specificEpithet" in r:
            rt = {
                "dwc:genus": r["dwc:genus"],
                "dwc:specificEpithet": r["dwc:specificEpithet"]
            }
            match = fuzzy_wuzzy_string(r["dwc:genus"] + " " + r["dwc:specificEpithet"], rank="species", should=should)
            match_new = fuzzy_wuzzy_string_new(r["dwc:genus"] + " " + r["dwc:specificEpithet"], rank="species", should=should)
        elif "dwc:scientificName" in r:
            rank = None
            if "dwc:taxonRank" in r:
                cand_rank = r["dwc:taxonRank"].lower()
                if cand_rank in taxon_rank.acceptable:
                    rank = cand_rank
                elif cand_rank in taxon_rank.mapping:
                    rank = taxon_rank.mapping[cand_rank]
                else:
                    print "unkown rank:", cand_rank

            if rank is None:
                if len(r["dwc:scientificName"].split()) == 1:
                    rank = "genus"
                else:
                    rank = "species"

            rt = {
                "dwc:scientificName": r["dwc:scientificName"]
            }
            match = fuzzy_wuzzy_string(r["dwc:scientificName"], rank=rank, should=should)
            match_new = fuzzy_wuzzy_string_new(r["dwc:scientificName"], rank=rank, should=should)
        elif "dwc:genus" in r:
            rt = {
                "dwc:genus": r["dwc:genus"]
            }
            match = fuzzy_wuzzy_string(r["dwc:genus"], rank="genus", should=should)
            match_new = fuzzy_wuzzy_string_new(r["dwc:genus"], rank="genus", should=should)
        else:
            print r
            return ("failout", (None, None))

        if match_new is not None:
            if match is not None:
                if match["dwc:taxonID"] != match_new["dwc:taxonID"]:
                    return ("rematch", (rt, match_new, match))
                else:
                    return ("match", (rt, match_new))
            else:
                return ("newmatch", (rt, match_new))
        else:
            if match is not None:
                return ("dematch", (rt, {"flag_taxon_match_failed": True}, match))
            else:
                return ("nomatch", (rt, {"flag_taxon_match_failed": True}))
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
            "data.dwc:taxonRank",
            "data.dwc:kingdom",
            "data.dwc:phylum",
            "data.dwc:class",
            "data.dwc:order",
        ]
    }

    for r in elasticsearch.helpers.scan(es, index="idigbio", query=body, size=1000, doc_type=t, scroll="10m"):
        etag = objectHasher("sha256", r["_source"], sort_arrays=True)
        stats["count"] += 1
        if etag not in etags:
            stats["precount"] += 1
            etags.add(etag)
            yield (etag,r["_source"]["data"])

def test_main():
    global DEBUG
    DEBUG = True
    tests = [
        # { # Works, no match
        #     "dwc:genus": "Eupsophus",
        #     "dwc:specificEpithet": "juniensis"
        # },
        # { # Data fail, name not actually genus
        #     "dwc:genus": "Brachyura",
        # },
        # { # Fail, name not in backbone
        #      "dwc:scientificName": "Squalodon errabundus"
        # },
        # { # Works
        #     "dwc:genus": "Caulerpa",
        #     "dwc:specificEpithet": "acuta"
        # },
        # { # Works
        #     "dwc:genus": "Caulerpa",
        #     "dwc:specificEpithet": "filicoides"
        # },
        # {  # Fail, Fucus crispatus != Fucus crispus
        #     "dwc:scientificName": "Fucus crispus",
        # },
        # { # Works
        #     "dwc:genus": "Aronia",
        #     "dwc:specificEpithet": "arbutifolia",
        # },
        # { # Works
        #     "dwc:genus": "Photinia",
        #     "dwc:specificEpithet": "pyrifolia",
        # },
        # { # Works
        #     "dwc:genus": "Pseudoliomera",
        #     "dwc:specificEpithet": "granosimana",
        # },
        # { # Works, synonym
        #     "dwc:specificEpithet": "albellum",
        #     "dwc:genus": "Leccinum",
        # },
        # { # Fail, not in GBIF taxonomy, possible candidate for improving failure modes
        #     "dwc:specificEpithet": "epidermata",
        #     "dwc:order": "Cystoporida",
        #     "dwc:kingdom": "Animalia",
        #     "dwc:phylum": "Bryozoa",
        #     "dwc:genus": "Favositella",
        #     "dwc:family": "Ceramoporidae",
        #     "dwc:scientificName": "Favositella epidermata"
        # },
        {  # Data fail - scientificName is a subclass
            "dwc:kingdom": "Animalia",
            "dwc:phylum": "Mollusca",
            "dwc:class": "Cephalopoda",
            "dwc:scientificName": "Ammonoidea"
        },
        {  # Purposeful failure - backbone does not contain subclasses
            "dwc:kingdom": "Animalia",
            "dwc:phylum": "Mollusca",
            "dwc:class": "Cephalopoda",
            "dwc:scientificName": "Ammonoidea",
            "dwc:taxonRank": "subclass"
        },
    ]
    for i, t in enumerate(tests):
        print(work((str(i), t)))


import numpy as np
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
def show_plot():
    labels, values = zip(*score_stats.items())

    indexes = np.arange(len(labels))
    width = 1

    plt.bar(indexes, values, width)
    plt.xticks(indexes + width * 0.5, labels)
    plt.savefig("plot")

def main():
    p = pool.Pool(25)

    with open("taxon_kv.txt", "wb") as outf:
        for r in result_collector(p.imap_unordered(work,get_taxon_from_index())):
            if r[0] is not None:
                outf.write(json.dumps(list(r)) + "\n")

    print(stats.most_common())

    # with open("search_cache.json", "wb") as pp:
    #     json.dump(search_cache,pp)


if __name__ == '__main__':

    if len(sys.argv) > 1 and sys.argv[1] == "test":
        test_main()
    else:
        main()
