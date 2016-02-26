from locality_data import *


def get_data():
    aggs_fields = ["continent", "country", "stateprovince"]
    agg_remap = {
        "continent": "dwc:continent",
        "country": "dwc:country",
        "stateprovince": "dwc:stateProvince"
    }

    vds = {}
    for k in implied_parent:
        vds[agg_remap[k]] = {}
        for v in implied_parent[k]:
            vds[agg_remap[k]][v] = {}
            for i, p in enumerate(implied_parent[k][v]):
                vds[agg_remap[k]][v][agg_remap[aggs_fields[i]]] = p

    for k in string_to_iso_code:
        if k in vds["dwc:country"]:
            vds["dwc:country"][k]["idigbio:isoCountryCode"] = string_to_iso_code[k]
        else:
            vds["dwc:country"][k] = {"idigbio:isoCountryCode": string_to_iso_code[k]}

    to_insert = []
    for k in vds:
        for v, o in vds[k].iteritems():
            if "dwc:country" in o and o["dwc:country"] in vds["dwc:country"]:
                o.update(vds["dwc:country"][o["dwc:country"]])
            to_insert.append((dict([[k, v]]), o, "data_dictionaries_1", True))

    for k in kl:
        for v in kl[k]:
            kd = dict([[agg_remap[k], v.encode("utf-8")]])
            vd = dict([[agg_remap[k], ""]])
            if kl[k][v] != "None":
                vd = dict([[agg_remap[k], kl[k][v]]])

            for k2 in vd.keys():
                if k2 in vds and vd[k2] in vds[k2]:
                    vd.update(vds[k2][vd[k2]])

            to_insert.append((kd, vd, "data_dictionaries_2", True))

    return to_insert


def get_sources():
    return ["data_dictionaries_1", "data_dictionaries_2"]
