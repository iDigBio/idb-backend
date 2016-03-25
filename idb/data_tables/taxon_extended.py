import json

def get_data():
    with open("taxon_kv.txt") as tkvf:
        for l in tkvf:
            k, v = json.loads(l)
            yield (k, v, 'gbif_checklist_extended', True)

def get_sources():
    return ["gbif_checklist_extended"]