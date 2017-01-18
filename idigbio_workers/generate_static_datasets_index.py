from __future__ import division, absolute_import, print_function
import os
import sys
import uuid
import csv
import datetime
import dateutil.parser
import requests
import json
import re
from cStringIO import StringIO


from idb.helpers.storage import IDigBioStorage


def is_uuid(s):
    try:
        uuid.UUID(s)
        return True
    except:
        return False

ic_replacement_table = {}
ic_extract = re.compile("^.*\(([a-zA-Z]+)\)$")


def get_true_ic(v):
    m = ic_extract.match(v)
    if m is not None:
        v = m.groups()[0]
    v = v.lower().strip()
    if v in ic_replacement_table:
        v = ic_replacement_table[v]
    return v


def main():
    index_file_name = "index.txt"

    query = {
        "size": 0,
        "aggs": {
            "rs": {
                "terms": {
                    "field": "recordset",
                    "size": 1000
                },
                "aggs": {
                    "ic":{
                        "terms": {
                            "field": "institutioncode",
                            "size": 1000,
                        },
                        "aggs": {
                            "cc": {
                                "terms":{
                                    "field": "collectioncode",
                                    "size": 1000,
                                }
                            }
                        }
                    }
                }
            }
        }
    }
    r = requests.post("http://search.idigbio.org/idigbio/records/_search",
                      data=json.dumps(query),
                      headers={"Content-Type": "application/json"})
    r.raise_for_status()
    ro = r.json()

    recordsets = {}
    for rs_b in ro["aggregations"]["rs"]["buckets"]:
        rsid = rs_b["key"]
        ic = ""
        cc = ""
        if len(rs_b["ic"]["buckets"]) == 0:
            ic = ""
            cc = ""
        elif len(rs_b["ic"]["buckets"]) == 1 or (
                float(rs_b["ic"]["buckets"][0]["doc_count"]) / float(rs_b["doc_count"]) > 0.9
            ):
            ic_b = rs_b["ic"]["buckets"][0]
            ic = get_true_ic(ic_b["key"])
            if len(ic_b["cc"]["buckets"]) == 0:
                cc = ""
            elif len(ic_b["cc"]["buckets"]) == 1:
                cc = ic_b["cc"]["buckets"][0]["key"]
            else:
                cc = "MULTIPLE"
        else:
            # print(rs_b)
            ic = "MULTIPLE"
            cc = "MULTIPLE"
        recordsets[rsid] = {
            "institutioncode": ic,
            "collectioncode": cc
        }

    s = IDigBioStorage()
    b = s.get_bucket("idigbio-static-downloads")

    headers = ["zipfile","emlfile","etag","modified","recordset_id", "institutioncode", "collectioncode"]
    files = {}

    for k in b.list():
        # Skip the index itself
        if k == index_file_name:
            continue

        # Skip files older than 8 days
        lm_d = dateutil.parser.parse(k.last_modified).date()
        if lm_d < (datetime.datetime.now() - datetime.timedelta(7)).date():
            continue

        fkey = k.name.split(".")[0]
        if fkey not in files:
            files[fkey] = {k:"" for k in headers}

        if k.name.endswith(".eml"):
            files[fkey]["emlfile"] = k.name
        elif k.name.endswith(".zip"):
            files[fkey]["zipfile"] = k.name
            files[fkey]["modified"] = k.last_modified
            files[fkey]["etag"] = k.etag
            if is_uuid(fkey):
                files[fkey]["recordset_id"] = fkey
                if fkey in recordsets:
                    files[fkey]["institutioncode"] = recordsets[fkey]["institutioncode"]
                    files[fkey]["collectioncode"] = recordsets[fkey]["collectioncode"]
                else:
                    files[fkey]["institutioncode"] = ""
                    files[fkey]["collectioncode"] = ""

    fil = StringIO()

    cw = csv.writer(fil,delimiter="\t")

    cw.writerow(headers)
    for k in files:
        if files[k]["zipfile"] != "":
            cw.writerow([files[k][h].replace("\"","") for h in headers])

    fil.seek(0)

    ik = b.get_key(index_file_name,validate=False)
    ik.content_type = 'text/tsv'
    ik.set_contents_from_file(fil)
    ik.make_public()


if __name__ == '__main__':
    main()
