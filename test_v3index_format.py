import json

from datetime import datetime, date

def json_serial(obj):
    """JSON serializer for objects not serializable by default json code"""

    if isinstance(obj, datetime):
        serial = obj.isoformat()
        return serial
    elif isinstance(obj, date):
        serial = obj.isoformat()
        return serial    
    raise TypeError ("Type {} not serializable".format(type(obj)))

from collections import defaultdict

from idb.helpers.fieldnames import sub_types, type_list
from idb.postgres_backend import apidbpool, NamedTupleCursor
from idb.corrections.record_corrector import RecordCorrector
from idb.helpers.conversions import grabAll

# TODO: Need to handle keys that are already typed arrays
def create_typed_dict(r):
    d = defaultdict(dict)
    for f in r:            
        #print(f,res.data[f],f in sub_types)
        if f in sub_types:
            if sub_types[f] is not None:
                # This is already a class entry
                if f.lower() == sub_types[f].lower() or isinstance(r[f],list):
                    d[sub_types[f]] = r[f]
                else:
                    d[sub_types[f]][f] = r[f]
            else:
                d[f] = r[f]
        else:
            d[f] = r[f]
    return d

def mangle(k):
    return k.replace(":","_").lower()

def main():
    # from idb.helpers.conversions import fields, custom_mappings
    # for t in type_list:
    #     for k in type_list[t]:
    #         m = {
    #             "date_detection": False,
    #             "properties": {}
    #         }            
    #         if f[2] == "text":
    #             m["properties"][f[0]] = {
    #                 "type": "string", "analyzer": "keyword"}
    #         elif f[2] == "longtext":
    #             m["properties"][f[0]] = {"type": "string"}
    #         elif f[2] == "list":
    #             m["properties"][f[0]] = {
    #                 "type": "string", "analyzer": "keyword"}
    #         elif f[2] == "float":
    #             m["properties"][f[0]] = {"type": "float"}
    #         elif f[2] == "boolean":
    #             m["properties"][f[0]] = {"type": "boolean"}
    #         elif f[2] == "integer":
    #             m["properties"][f[0]] = {"type": "integer"}
    #         elif f[2] == "date":
    #             m["properties"][f[0]] = {"type": "date"}
    #         elif f[2] == "point":
    #             m["properties"][f[0]] = {
    #                 "type": "geo_point",
    #                 "geohash": True,
    #                 "geohash_prefix": True,
    #                 "lat_lon": True
    #             }
    #         elif f[2] == "shape":
    #             m["properties"][f[0]] = {"type": "geo_shape"}
    #         elif f[2] == "custom":
    #             m["properties"][f[0]] = custom_mappings[t][f[0]]

    # r_mappings = {}

    # rc = RecordCorrector()
    # sql = "SELECT * FROM idigbio_uuids_data WHERE type=%s AND deleted=false LIMIT 10"
    # results = apidbpool.fetchiter(sql, ("record",), named=True, cursor_factory=NamedTupleCursor)
    # for res in results:
    #     d = create_typed_dict(res.data)

    #     for k in d:
    #         dk = d[k]
    #         if isinstance(dk,dict):
    #             dk["idigbio:source"] = "raw"
    #             d[k] = [dk]
    #         elif isinstance(dk,list):
    #             for dki in dk:
    #                 dki["idigbio:source"] = "raw"
    #         else:
    #             print("raw", k, dk)

    #     correction_list = rc.get_correction_list(res.data)

    #     for cr, source in correction_list:
    #         crd = create_typed_dict(cr)
    #         for k in crd:
    #             crdk = crd[k]
    #             if k in d:
    #                 for dki in d[k]:
    #                     if dki.get("idigbio:source") == "raw":
    #                         crdk, _ = rc.correct_record(dki,correction_list=[(crdk,source)])

    #                         break
    #             if isinstance(crdk,dict):
    #                 crdk["idigbio:source"] = source                    
    #                 if k in d:
    #                     d[k].append(crdk)
    #                 else:
    #                     d[k] = [crdk]
    #             elif isinstance(crdk,list):
    #                 if k in d:
    #                     d[k].extend(crdk)
    #                 else:
    #                     d[k] = crdk
    #             else:
    #                 print("corr", k, crdk)

    #     r = {}
    #     for k in d:
    #         r_munged_key = k.replace(":","_").lower()
    #         r[r_munged_key] = []
    #         if r_munged_key not in r_mappings:
    #             r_mappings[r_munged_key] = {}

    #         for dki in d[k]:
    #             # TODO: Build new idq based conversion that rely only on DWC terms.
    #             rki = grabAll("records", dki)
    #             for rkik in list(rki.keys()):
    #                 if rki[rkik] is None:
    #                     del rki[rkik]
    #                 elif isinstance(rki[rkik],list) and len(rki[rkik]) == 0:
    #                     del rki[rkik]
    #                 else:
    #                     r_mappings[r_munged_key][rkik] = "text"

    #             r[r_munged_key].append(rki)

    #     print(json.dumps(res.data, indent=4, sort_keys=True))
    #     print(json.dumps(d, indent=4, sort_keys=True))
    #     print(json.dumps(r, indent=4, sort_keys=True, default=json_serial))
    #     print("-------------------------------")

    # print json.dumps(r_mappings, indent=4, sort_keys=True)

    t = {}
    for k, v in sub_types.items():
        if mangle(v) not in t:
            t[mangle(v)] = {}
        t[mangle(v)][mangle(k)] = {}

    print(json.dumps(t, indent=4, sort_keys=True))



if __name__ == '__main__':
    main()