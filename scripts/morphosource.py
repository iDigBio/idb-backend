import csv
import idigbio

api = idigbio.json()

def check_match(rq, d):
    r = api.search_records(rq=rq, fields_exclude=[])
    if len(r["items"]) == 1:
        item = r["items"][0]
        if (
            (
                d["Genus"] == "" and d["Species"] == ""
            ) or (
                item["data"].get("dwc:genus", "").lower() == d["Genus"].lower() and
                item["data"].get("dwc:specificEpithet", "").lower() == d["Species"].lower()
            ) or (
                item["indexTerms"].get("genus", "").lower() == d["Genus"].lower() and
                item["indexTerms"].get("specificepithet", "").lower() == d["Species"].lower()
            ) or (
                item["data"].get("dwc:scientificName", "").lower().startswith(d["Genus"].lower() + " " + d["Species"].lower())
            ) or (
                item["indexTerms"].get("scientificname", "").lower().startswith(d["Genus"].lower() + " " + d["Species"].lower())
            )
        ):
            return [item["uuid"], item["data"].get("dwc:occurrenceID",""), ""]
        else:
            print(d)
            print(item["data"].get("dwc:genus"), item["data"].get("dwc:specificEpithet"))
            print(item["indexTerms"].get("genus"), item["indexTerms"].get("specificepithet"))
            print(item["data"].get("dwc:scientificName"), item["indexTerms"].get("scientificname"))
            return [item["uuid"], item["data"].get("dwc:occurrenceID",""), "name"]

def morpho_match(inst, cat_prefix, additional_params={}, ccmap={}, filename=None):
    if filename is None:
        uuid_filename = "Downloads/{}_uuids.txt".format(inst)
        filename = "Downloads/{}.txt".format(inst)
    else:
        fa = filename.rsplit(".", 1)
        uuid_filename = fa[0] + "_uuids." + fa[1]

    with open(uuid_filename, "w") as outf:
        cw = csv.writer(outf)
        with open(filename, "r") as inf:
            cr = csv.reader(inf, delimiter="\t")
            header = cr.next()
            cw.writerow(header + ["uuid", "occurenceID", "Failure Reason"])
            for l in cr:
                d = dict(zip(header, l))
                if "-" in d["Catalog number"]:
                    rq = {
                        "institutioncode": d["Institution code"].lower(),
                        "catalognumber": [
                            cat_prefix + d["Catalog number"].strip(),
                            cat_prefix + d["Catalog number"].strip().split("-", 1)[0],
                        ]
                    }
                else:
                    rq = {
                        "institutioncode": d["Institution code"].lower(),
                        "catalognumber": cat_prefix + d["Catalog number"].strip(),
                    }

                rq.update(additional_params)
                if d["Collection code"] in ccmap:
                    rq.update(ccmap[d["Collection code"]])
                rq_genus = {}
                rq_genus.update(rq)
                rq_genus["genus"] = d["Genus"].lower()
                rv = check_match(rq, d)
                if rv is None:
                    rv = check_match(rq_genus, d)

                if rv is None:
                    cw.writerow(l + ["", "", "nomatch"])
                    print(d)
                else:
                    cw.writerow(l + rv)


# morpho_match("amnh.txt", "M-")
# morpho_match("MCZ", "", {"collectioncode": "Mamm"})
# morpho_match("UCMP", "", {"collectioncode": "V"})
# morpho_match("USNM", "PAL")

morpho_match("UF", "", ccmap={
    "I": {"collectioncode": "Fish"},
    "H": {"collectioncode": "Herp"},
    "VP": {"collectioncode": "UF"},
    "Mammals": {"collectioncode": "Mammals"},
    "M": {"collectioncode": "Mammals"},
    "TRO": {"collectioncode": "UF/TRO"},
    "IGM": {"collectioncode": "UF/IGM"}
}, filename="Downloads/UF_10_16_17.txt")

#morpho_match("UCMP", "", {"collectioncode": "V"}, filename="Downloads/UCMP_specimens_no_taxonomy.txt")
