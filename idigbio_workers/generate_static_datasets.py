import os
import sys

sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.realpath(__file__)))))

import requests
import json
import datetime

from idigbio_workers.lib.download import generate_files, get_recordsets, es, indexName
from idigbio_workers.lib.query_shim import queryFromShim
from idb.helpers.storage import IDigBioStorage
#from idb.lib.data.export.eml import eml_from_recordset

def runQuery(query):
    return es.search(index=indexName,doc_type="record,mediarecords",body=query)

def upload_download_file_to_ceph(s, dsname):
    keyname = dsname
    makelink = False
    if dsname == "idigbio.zip":
        t = datetime.date.today().isoformat()
        keyname = "idigbio-" + t + ".zip"
        makelink = True
    fkey = s.upload_file(keyname, "idigbio-static-downloads", dsname)

    fkey.set_metadata('Content-Type', 'application/zip')
    os.unlink(dsname)

    if makelink:
        fkey.copy("idigbio-static-downloads", dsname, preserve_acl=True)

    return "http://s.idigbio.org/idigbio-static-downloads/" + keyname

def upload_eml_file_to_ceph(s, tid, eml):
    fkey = s.get_key(tid + ".eml","idigbio-static-downloads")
    fkey.set_contents_from_string(eml)
    fkey.set_metadata('Content-Type', 'application/xml')
    fkey.make_public()
    return "http://s.idigbio.org/idigbio-static-downloads/" + tid + ".eml"


def main():
    s = IDigBioStorage()
    # static_queries = [
    #     ({},"idigbio"),
    #     ({"hasImage": True},"idigbio-images"),
    # ]
    # rsquery = {
    #     "query": {
    #         "match_all": {}
    #     },
    #     "size": 0,
    #     "aggs": {
    #         "recordset_counts": {
    #             "terms": {
    #                 "field": "recordset",
    #                 "size": 10000
    #             }
    #         }
    #     }
    # }
    # ro = runQuery(rsquery)
    # if ro is not None:
    #     for b in ro["aggregations"]["recordset_counts"]["buckets"]:
    #         #print b["key"], b["doc_count"], b["doc_count"] * 7 / 10000
    #         static_queries.append(({
    #             "recordset": b["key"]
    #         },b["key"]))

    # count = 0
    # for q in reversed(static_queries):
    #     print count, q
    #     file_name, _, _ = generate_files(record_query=queryFromShim(q[0])["query"],form="dwca-csv",filename=q[1])
    #     print q[1], file_name
    #     u = upload_download_file_to_ceph(s,file_name)
    #     # # rseml = eml_from_recordset(q[1],env="prod")
    #     # # e = upload_eml_file_to_ceph(s,q[1],rseml)
    #     print q[1], u
    #     count += 1
    file_name, _, _ = generate_files(record_query=queryFromShim({})["query"], form="dwca-csv", filename="idigbio")
    u = upload_download_file_to_ceph(s, file_name)

if __name__ == '__main__':
    main()
