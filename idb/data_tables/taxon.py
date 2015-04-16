from idb.lib.data.importers.dwca import Dwca

whitelist = [
    #"description.txt",
    #"distribution.txt",
    #"image.txt",
    #"reference.txt",
    #"speciesprofile.txt",
    #"taxon.txt",
    #"typesandspecimen.txt",
    #"vernacularname.txt"
]

fields = [
    "dwc:kingdom",
    "dwc:phylum",
    "dwc:class",
    "dwc:order",
    "dwc:family",
    "dwc:taxonRank",
    "dwc:taxonomicStatus",
    "gbif:canonicalName"
]

def prune_record(r):
    d = {}
    for f in fields:
        if f in r:
            d[f] = r[f].split(" ")[0]
    return d

def get_data(path_to_checklist):
    d = Dwca(path_to_checklist,logname="idigbio")

    taxa_lines = []
    higher_taxons = ["dwc:kingdom","dwc:phylum","dwc:class","dwc:order","dwc:superfamily"]
    for r in d.core:
        if r["dwc:taxonRank"] == "family":
            r = prune_record(r) 
            if r["dwc:taxonomicStatus"] == "accepted":
                hta = {}
                skiprest = False
                for ht in higher_taxons:
                    if ht in r:
                        hta[ht] = r[ht]                
                taxa_lines.append(({"dwc:family": r["gbif:canonicalName"]},hta,"gbif_checklist",True))
    return taxa_lines

def get_sources():
    return ["gbif_checklist"]