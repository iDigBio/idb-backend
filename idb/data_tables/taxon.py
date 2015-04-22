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

def get_data(path_to_checklist):
    d = Dwca(path_to_checklist,logname="idigbio")

    sp_count = 0
    sp_non_count = 0
    fail = 0
    names = {}
    ids = {}
    needs_name = []
    taxa_lines = []
    higher_taxons = ["dwc:kingdom","dwc:phylum","dwc:class","dwc:order","dwc:superfamily"]
    for r in d.core:
        try:
            if r["dwc:taxonRank"] == "family":
                if r["dwc:taxonomicStatus"] == "accepted":
                    hta = {}
                    skiprest = False
                    for ht in higher_taxons:
                        if ht in r:
                            hta[ht] = r[ht]
                    taxa_lines.append(({"dwc:family": r["gbif:canonicalName"]},hta,"gbif_checklist",True))
            if r["dwc:taxonRank"] == "species":
                if "dwc:genus" in r and "dwc:specificEpithet" in r:
                    g = r["dwc:genus"].lower()
                    s = r["dwc:specificEpithet"].lower()
                    id = r["id"]

                    if r["dwc:taxonomicStatus"] == "accepted":
                        sp_count += 1
                        ids[id] = (g,s)
                        gbif_t = {
                            "gbif:genus": g,
                            "gbif:specificEpithet": s,
                            "gbif:taxonID": id,
                            "gbif:cannonicalName": r["gbif:canonicalName"].lower()
                        }
                        for ht in higher_taxons + ["dwc:family"]:
                            if ht in r:
                                gbif_t[ht] = r[ht]
                        names[(g,s)] = gbif_t
                    elif "dwc:acceptedNameUsageID" in r:
                        sp_non_count += 1
                        if r["dwc:acceptedNameUsageID"] in ids:
                            names[(g,s)] = names[ids[r["dwc:acceptedNameUsageID"]]]
                        else:
                            needs_name.append((g,s,r["dwc:acceptedNameUsageID"]))
                    else:
                        fail += 1
                else:
                    fail += 1
        except:
            print r

    #print len(names), len(needs_name)
    total_miss = 0
    for g,s,id in needs_name:
        if id in ids:
            names[(g,s)] = names[ids[id]]
        else:
            total_miss += 1

    #print sp_count, sp_non_count, fail, total_miss

    for g,s in names:
        taxa_lines.append(({"dwc:genus": g, "dwc:specificEpithet": s},names[(g,s)],"gbif_checklist_gs",True))

    return taxa_lines

def get_sources():
    return ["gbif_checklist","gbif_checklist_gs"]

def main():
    count = 0
    for dr in get_data("/home/godfoder/taxon/checklist1.zip"):
        count += 1
        #print dr
    print count

if __name__ == '__main__':
    main()