import json
from collections import Counter
from idb.helpers.etags import objectHasher

def get_data():

    keys = {}
    values = {}
    with open("taxon_kv.txt") as tkvf:
        for l in tkvf:
            # There could be 2 or more elements of o, so we cant do a simple assignment
            o = json.loads(l)
            k = o[0]
            v = o[1]

            k_etag = objectHasher("sha256", k, sort_arrays=True)
            if k_etag not in keys:
                keys[k_etag] = (k, Counter())

            v_etag = objectHasher("sha256", v, sort_arrays=True)
            if v_etag not in values:
                values[v_etag] = v

            keys[k_etag][1][v_etag] += 1

    for k_etag, (k, value_counts) in keys.items():
        v = values[value_counts.most_common(1)[0][0]]

        if "dwc:scientificName" in v:
            v["gbif:canonicalName"] = v["dwc:scientificName"]
            del v["dwc:scientificName"]

        if "flag_taxon_match_failed" not in v:
            v["flag_gbif_taxon_corrected"] = True

        yield (k, v, 'gbif_checklist_extended', True)

def get_sources():
    return ["gbif_checklist_extended"]
