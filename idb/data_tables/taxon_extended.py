import json
from collections import Counter
from idb.helpers.etags import objectHasher

failure_dict = {"flag_taxon_match_failed": True}

def get_data():

    keys = {}
    values = {}

    stats = Counter()

    with open("quartiles.json", "r") as qf:
        quartiles = json.load(qf)

    with open("taxon_kv.txt") as tkvf:
        for l in tkvf:
            # There could be 2 or more elements of o, so we cant do a simple assignment
            o = json.loads(l)
            data_etag = o[0]
            result = o[1]
            k = o[2][0]
            v = o[2][1]
            score = o[3]
            hits = o[4]

            # Reject scores below the first quartile
            if score < quartiles[0]:
                result = "nomatch"
                v = failure_dict

            stats["count"] += 1
            if result == "match":
                stats["matched"] += hits
            elif result == "nomatch":
                stats["failed"] += hits

            k_etag = objectHasher("sha256", k, sort_arrays=True)
            if k_etag not in keys:
                keys[k_etag] = (k, Counter())

            v_etag = objectHasher("sha256", v, sort_arrays=True)
            if v_etag not in values:
                values[v_etag] = v

            keys[k_etag][1][v_etag] += score

            if stats["count"] % 10000 == 0:
                print(stats.most_common())

    for k_etag, (k, value_counts) in keys.items():
        v = values[value_counts.most_common(1)[0][0]]

        if "dwc:scientificName" in v:
            # Canonical name is back in the backbone.
            # v["gbif:canonicalName"] = v["dwc:scientificName"]
            del v["dwc:scientificName"]

        if "flag_taxon_match_failed" not in v:
            v["flag_gbif_taxon_corrected"] = True

        yield (k, v, 'gbif_checklist_extended', True)

def get_sources():
    return ["gbif_checklist_extended"]
