from __future__ import division, absolute_import
from gevent import monkey, pool
monkey.patch_all()

import os
import sys
import json
import traceback
from collections import Counter
import pprint

import re
pattern = re.compile('[\W_0-9]+|sp.')

import elasticsearch.helpers
from elasticsearch import Elasticsearch

from idb.helpers.etags import objectHasher
import idb.data_tables.taxon_rank as taxon_rank

DEBUG = False

class TaxonRankError(Exception):
    pass


# CUTOFF = 3.0

es = Elasticsearch([
    "c18node2.acis.ufl.edu",
    "c18node6.acis.ufl.edu",
    "c18node10.acis.ufl.edu",
    "c18node12.acis.ufl.edu",
    "c18node14.acis.ufl.edu"
], sniff_on_start=False, sniff_on_connection_fail=False, retry_on_timeout=True, max_retries=10, timeout=30)

last_run = {}

etags = Counter()

# search_cache = {}
# if os.path.exists("search_cache.json"):
#     with open("search_cache.json", "rb") as pp:
#         search_cache = json.load(pp)
# print "Search Cache: {0}".format(len(search_cache)),

taxon_data_fields = ["dwc:kingdom", "dwc:phylum", "dwc:class", "dwc:order", "dwc:family", "dwc:genus", "dwc:specificEpithet", "dwc:infraSpecificEpithet", "dwc:scientificName", "dwc:scientificNameAuthorship", "dwc:verbatimTaxonRank"]

good_status = {"accepted", "synonym"}
score_stats = Counter()
def run_query(q, cache_string, log_score=True):
    rsp = es.search(index="taxonnames",doc_type="taxonnames",body=q)

    best_response = None
    for h in rsp["hits"]["hits"]:
        if best_response is None:
            best_response = h
        else:
            if h["_score"] > best_response["_score"]:
                best_response = h

    #search_cache[cache_string] = None
    if best_response is not None:
        if log_score:
            score_stats[round(best_response["_score"], 1)] += 1  # pylint: disable=round-builtin

        # Reject low quality matches
        # Disable fixed cutoff here, moving to loader script and using first quartile
        # if best_response["_score"] <= CUTOFF:
        #     return (None, best_response["_score"])

        if DEBUG:
            pp = pprint.PrettyPrinter(indent=2)
            pp.pprint(best_response)
            print("\n\n")
            print("synonym" in best_response["_source"]["dwc:taxonomicStatus"])
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
                return (rsp["hits"]["hits"][0]["_source"], best_response["_score"])
            else:
                return (None, -1)
        else:
            return (best_response["_source"], best_response["_score"])
    else:
        return (None, -1)


# Limit to only the kingdoms we're likely to have specimens for.
core_kingdoms = ["animalia", "plantae", "fungi", "chromista", "protista", "protozoa"]

def fuzzy_wuzzy_string_new(s, rank="species", should=None):
    # if s in search_cache:
    #     return search_cache[s]

    if "dwc:kingdom" in should and should["dwc:kingdom"].lower() in core_kingdoms:
        kingdom_restriction = {
            "term": {
                "dwc:kingdom": should["dwc:kingdom"].lower()
            }
        }
        del should["dwc:kingdom"]
    else:
        kingdom_restriction = {"or": []}
        for k in core_kingdoms:
            kingdom_restriction["or"].append({
                "term": {
                    "dwc:kingdom": k
                }
            })

    q = {
        "query": {
            "bool": {
                "must": [
                    {
                        "match": {
                            "gbif:canonicalName": {
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
                    kingdom_restriction
                ]
            }
        }
    }

    if should is not None:
        q["query"]["bool"]["should"] = []
        shd = q["query"]["bool"]["should"]
        for k in should:
            if k != "dwc:scientificName":
                shd.append({
                    "match": {
                        k: {
                            "query": should[k],
                            "fuzziness": "AUTO"
                        }
                    }
                })

    if DEBUG:
        print(json.dumps(q, indent=2))

    return run_query(q, s.lower())


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
        stats[r[1]] += 1
        stats["postcount"] += 1

        if stats["postcount"] % 10000 == 0:
            print(stats.most_common(), score_stats.most_common())
            show_plot()

        yield r

def work(t):
    etag, r = t
    try:
        rt = None
        match = None

        should = {}

        if "dwc:verbatimTaxonRank" in r:
            if "dwc:taxonRank" not in r:
                r["dwc:taxonRank"] = r["dwc:verbatimTaxonRank"]
            del r["dwc:verbatimTaxonRank"]

        for k in taxon_data_fields:
            if k in r:
                should[k] = r[k]

        if "dwc:genus" in r and "dwc:specificEpithet" in r and r["dwc:specificEpithet"] not in ["sp.", "sp"]:
            rt = {
                "dwc:genus": r["dwc:genus"],
                "dwc:specificEpithet": r["dwc:specificEpithet"]
            }
            match, score = fuzzy_wuzzy_string_new(r["dwc:genus"] + " " + r["dwc:specificEpithet"], rank="species", should=should)
        elif "dwc:scientificName" in r:
            rank = None

            if "dwc:taxonRank" in r:
                cand_rank = r["dwc:taxonRank"].lower()
                if cand_rank in taxon_rank.acceptable:
                    rank = cand_rank
                elif cand_rank in taxon_rank.mapping:
                    rank = taxon_rank.mapping[cand_rank]
                else:
                    print ("unknown rank:", cand_rank)

            if rank is None:
                if r["dwc:scientificName"].endswith(" sp.") or r["dwc:scientificName"].endswith(" sp"):
                    rank = "genus"
                    r["dwc:scientificName"] = r["dwc:scientificName"].split(" ")[0]
                elif len(r["dwc:scientificName"].split()) == 1:
                    for k in taxon_data_fields:
                        if k == "dwc:scientificName":
                            continue

                        if r["dwc:scientificName"] == r.get(k,None):
                            rank = k.split(":")[1].lower()
                            if DEBUG:
                                print("rank: {}").format(rank)
                            break
                    else:
                        raise TaxonRankError("Failed to find rank for monomial")

                else:
                    rank = "species"

            rt = {
                "dwc:scientificName": r["dwc:scientificName"]
            }
            match, score = fuzzy_wuzzy_string_new(r["dwc:scientificName"], rank=rank, should=should)
        elif "dwc:genus" in r:
            rt = {
                "dwc:genus": r["dwc:genus"]
            }
            match, score = fuzzy_wuzzy_string_new(r["dwc:genus"], rank="genus", should=should)
        else:
            print (r)
            return (etag, "failout", (None, None), -1, etags[etag])

        if match is not None:
            return (etag, "match", (rt, match), score, etags[etag])
        else:
            return (etag, "nomatch", (rt, {"flag_taxon_match_failed": True}), score, etags[etag])
    except KeyboardInterrupt:
        raise KeyboardInterrupt
    except:
        traceback.print_exc()
        return (etag, "exception", (None, None), -1, etags[etag])

def get_taxon_from_index():
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
        "_source": ["data." + f for f in taxon_data_fields] + ["data.dwc:taxonRank"]
    }

    for r in elasticsearch.helpers.scan(es, index="idigbio", query=body, size=1000, doc_type=t, scroll="10m"):
        etag = objectHasher("sha256", r["_source"]["data"], sort_arrays=True)
        stats["count"] += 1
        if etag not in etags:
            stats["precount"] += 1
            try:
                yield (etag,r["_source"]["data"])
            except KeyError as e:
                print(r)
                raise e
        etags[etag] += 1

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
        # {  # Data fail - scientificName is a subclass
        #     "dwc:kingdom": "Animalia",
        #     "dwc:phylum": "Mollusca",
        #     "dwc:class": "Cephalopoda",
        #     "dwc:scientificName": "Ammonoidea"
        # },
        # {  # Purposeful failure - backbone does not contain subclasses
        #     "dwc:kingdom": "Animalia",
        #     "dwc:phylum": "Mollusca",
        #     "dwc:class": "Cephalopoda",
        #     "dwc:scientificName": "Ammonoidea",
        #     "dwc:taxonRank": "subclass"
        # },
        # {
        #     "dwc:specificEpithet": "humilis",
        #     "dwc:kingdom": "Plantae",
        #     "dwc:genus": "Tortella",
        #     "dwc:family": "Pottiaceae",
        #     "dwc:phylum": "Bryophyta",
        #     "dwc:class": "Bryopsida",
        #     "dwc:scientificName": "Tortella humilis",
        # },
        # {
        #     "dwc:specificEpithet": "serrulata",
        #     "dwc:order": "Brassicales",
        #     "dwc:kingdom": "Plantae",
        #     "dwc:genus": "Peritoma",
        #     "dwc:family": "Cleomaceae",
        #     "dwc:class": "Magnoliopsida",
        #     "dwc:scientificName": "Peritoma serrulata"
        # },
        # {
        #     "dwc:specificEpithet": "papposa",
        #     "dwc:kingdom": "Plantae",
        #     "dwc:genus": "Pectis",
        #     "dwc:family": "ASTERACEAE",
        #     "dwc:scientificName": "Pectis papposa"
        # },
        # {
        #     "dwc:specificEpithet": "acutangula",
        #     "dwc:kingdom": "Animalia",
        #     "dwc:genus": "Polydontes",
        #     "dwc:class": "Gastropoda",
        #     "dwc:family": "Camaenidae",
        # },
        # {
        #     "dwc:order": "Siluriformes",
        #     "dwc:genus": "Ictalurus",
        #     "dwc:class": "Actinopteri",
        #     "dwc:family": "Ictaluridae",
        #     "dwc:scientificName": "Ictalurus",
        # },
        # {
        #     "dwc:specificEpithet": "redacta",
        #     "dwc:kingdom": "Plantae",
        #     "dwc:order": "Rosales",
        #     "dwc:scientificNameAuthorship": "J. H. Ross",
        #     "dwc:genus": "Acacia",
        #     "dwc:family": "Fabaceae",
        #     "dwc:class": "Dicotyledonae",
        #     "dwc:scientificName": "Acacia redacta",
        # },
        # {
        #     "dwc:specificEpithet": "redacta",
        #     "dwc:kingdom": "Plantae",
        #     "dwc:scientificNameAuthorship": "J.H. Ross",
        #     "dwc:genus": "Acacia",
        #     "dwc:family": "Fabaceae",
        #     "dwc:scientificName": "Acacia redacta J.H. Ross",
        # },
        # {
        #     "dwc:specificEpithet": "laurocerasi",
        #     "dwc:kingdom": "Plantae",
        #     "dwc:genus": "Bacidia",
        #     "dwc:family": "Ramalinaceae",
        #     "dwc:scientificName": "Bacidia laurocerasi"
        # },
        # {
        #     "dwc:specificEpithet": "cataphractus",
        #     "dwc:kingdom": "Animalia",
        #     "dwc:order": "Scorpaeniformes",
        #     "dwc:genus": "Peristethus",
        #     "dwc:phylum": "Chordata",
        #     "dwc:class": "Actinopteri",
        #     "dwc:family": "Triglidae",
        #     "dwc:scientificName": "Peristethus cataphractus",
        # },
        # {
        #     "dwc:kingdom": "Animalia",
        #     "dwc:phylum": "Mollusca",
        #     "dwc:taxonRank": "subclass",
        #     "dwc:class": "Cephalopoda",
        #     "dwc:scientificName": "Ammonoidea"
        # },
        # {
        #     "dwc:verbatimTaxonRank": "genus",
        #     "dwc:scientificName": "Ammonoidea",
        # },
        # {
        #     "dwc:kingdom": "Animalia",
        #     "dwc:order": "Ammonoidea",
        #     "dwc:phylum": "Mollusca",
        #     "dwc:class": "Cephalopoda",
        #     "dwc:scientificName": "Ammonoidea",
        # },
        # {
        #     "dwc:order": "Perciformes",
        #     "dwc:kingdom": "Animalia",
        #     "dwc:genus": "Caranx",
        #     "dwc:family": "Carangidae",
        #     "dwc:phylum": "Chordata",
        #     "dwc:taxonRank": "genus",
        #     "dwc:class": "Actinopterygii",
        #     "dwc:scientificName": "Caranx sp.",
        # },
        # {
        #     "dwc:kingdom": "Animalia",
        #     "dwc:order": "Perciformes",
        #     "dwc:genus": "Caranx",
        #     "dwc:phylum": "Chordata",
        #     "dwc:class": "Actinopterygii",
        #     "dwc:family": "Carangidae",
        #     "dwc:scientificName": "Caranx sp."
        # }
        # {
        #     "dwc:specificEpithet": "capensis",
        #     "dwc:order": "Gadiformes",
        #     "dwc:scientificNameAuthorship": "(Kaup, 1858)",
        #     "dwc:kingdom": "Animalia",
        #     "dwc:genus": "Gaidropsarus",
        #     "dwc:family": "Lotidae",
        #     "dwc:phylum": "Chordata",
        #     "dwc:class": "Actinopterygii",
        #     "dwc:scientificName": "Gaidropsarus capensis"
        # },
        {
            "dwc:kingdom": "Animalia",
            "dwc:order": "Orthida",
            "dwc:genus": "Acosarina",
            "dwc:family": "Schizophoriidae",
            "dwc:phylum": "Brachiopoda",
            "dwc:class": "Rhynchonellata",
            "dwc:scientificName": "Acosarina"
        }
    ]
    for i, t in enumerate(tests):
        print(json.dumps(work((str(i), t)), indent=2))


import numpy as np
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
def show_plot():

    sorted_scores = sorted(score_stats.items())
    total = sum(score_stats.values())
    cumulative = 0
    # Calculate Q1-3
    quartiles = [0, 0, 0]
    min_k = 0  # Force 0 minimum
    #min_k = sorted_scores[0][0]
    max_k = sorted_scores[-1][0]
    for k, v in sorted_scores:
        if cumulative <= ((total+1)/4) <= (cumulative + v):
            quartiles[0] = k
        elif cumulative <= ((total+1)/2) <= (cumulative + v):
            quartiles[1] = k
        elif cumulative <= (3*(total+1)/4) <= (cumulative + v):
            quartiles[2] = k
            break
        cumulative += v

    labels = []
    values = []

    cur_val = min_k
    while cur_val <= max_k:
        labels.append(cur_val)
        if cur_val in score_stats:
            values.append(score_stats[cur_val])
        else:
            values.append(0)
        cur_val += 1

    indexes = np.arange(len(labels))

    plt.plot(indexes, values)
    #plt.axvline(x=CUTOFF*10, linewidth=1, color="red")
    for q in quartiles:
        plt.axvline(x=q, linewidth=1, color="green")
    plt.savefig("plot")
    plt.clf()

    with open("quartiles.json", "w") as qf:
        json.dump(quartiles, qf)

    print(quartiles, quartiles[2] - quartiles[0])

def main():
    p = pool.Pool(25)

    if os.path.exists("taxon_kv.txt"):
        print("Priming comparison table")
        with open("taxon_kv.txt", "rb") as inf:
            for l in inf:
                o = json.loads(l)
                if len(o) == 2:
                    etag = objectHasher("sha256", o[0], sort_arrays=True)
                    txid = o[1].get("dwc.taxonID", -1)
                    last_run[etag] = (txid, -2)
                elif len(o) == 4:
                    etag = o[0]
                    txid = o[2][1].get("dwc.taxonID", -1)
                    last_run[etag] = (txid, o[3])

        os.rename("taxon_kv.txt", "taxon_kv.txt.bak")

    print("Working")
    with open("taxon_kv.txt", "wb") as outf:
        if os.path.exists("name_cache.json"):
            print("Loading Cached Names")
            with open("name_cache.json", "r") as nc:
                names = json.load(nc)
        else:
            print("Building Name List")
            names = []
            count = 0
            for t in get_taxon_from_index():
                names.append(t)
                count += 1
                if count % 10000 == 0:
                    print(count)
            print("Saving Name Cache")
            with open("name_cache.json", "w") as nc:
                json.dump(names, nc)
        print("Resolving Names")
        for r in result_collector(p.imap_unordered(work,names)):
            if r[2][0] is not None:
                outf.write(json.dumps(list(r)) + "\n")

    # print(stats.most_common())

    # with open("search_cache.json", "wb") as pp:
    #     json.dump(search_cache,pp)


if __name__ == '__main__':

    if len(sys.argv) > 1 and sys.argv[1] == "test":
        test_main()
    else:
        main()
