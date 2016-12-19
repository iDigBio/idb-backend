import json

def get_data():
    with open("taxon_kv.txt") as tkvf:
        for l in tkvf:
            # There could be 2 or more elements of o, so we cant to a simple assignment
            o = json.loads(l)
            k = o[0]
            v = o[1]
            # correction_value = {
            #     "gbif:canonicalName": v["dwc:scientifcName"],
            #     "dwc:taxonomicStatus": v["dwc:taxonomicStatus"],
            #     "dwc:taxonID": v["dwc:taxonID"],
            #     "dwc:taxonRank": v["dwc:taxonRank"],
            #     "dwc:nameAccordingToID": "d7dddbf4-2cf0-4f39-9b2a-bb099caae36c",
            #     "dwc:nameAccordingTo": "GBIF Backbone Taxonomy",
            #     "gbif:VernacularName": v.get("gbif:VernacularName")
            # }
            if "dwc:scientificName" in v:
                v["gbif:canonicalName"] = v["dwc:scientificName"]
                del v["dwc:scientificName"]

            if "flag_taxon_match_failed" not in v:
                v["flag_gbif_taxon_corrected"] = True

            yield (k, v, 'gbif_checklist_extended', True)

def get_sources():
    return ["gbif_checklist_extended"]