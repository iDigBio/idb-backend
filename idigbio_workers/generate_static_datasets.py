from __future__ import division, absolute_import, print_function
import os

import datetime

from idb.indexing.indexer import get_connection, get_indexname

from idigbio_workers.lib.download import generate_files
from idigbio_workers.lib.query_shim import queryFromShim
from idb.helpers.storage import IDigBioStorage
#from idb.lib.data.export.eml import eml_from_recordset

def runQuery(query):
    return get_connection().search(index=get_indexname(), doc_type="records,mediarecords", body=query)

def upload_download_file_to_ceph(s, dsname):
    keyname = dsname
    makelink = False
    if dsname == "idigbio.zip":
        t = datetime.date.today().isoformat()
        keyname = "idigbio-" + t + ".zip"
        makelink = True
    fkey = s.upload(s.get_key(keyname, "idigbio-static-downloads"),
                    dsname, public=True, content_type='application/zip')
    #os.unlink(dsname)

    if makelink:
        import socket
        socket.setdefaulttimeout(300)
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
    #     ({"geopoint":{"type":"exists"},"taxonid":{"type":"exists"}},"idigbio-geotaxon")
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
    #     print(len(ro["aggregations"]["recordset_counts"]["buckets"]))
    #     for b in ro["aggregations"]["recordset_counts"]["buckets"]:
    #         #print(b["key"], b["doc_count"], b["doc_count"] * 7 / 10000)
    #         static_queries.append(({
    #             "recordset": b["key"]
    #         },b["key"]))

    # print(len(static_queries))
    # count = 0
    # for q in reversed(static_queries):
    #     print(count, q)
    #     file_name = generate_files(record_query=queryFromShim(q[0])["query"],form="dwca-csv",filename=q[1])
    #     print(q[1], file_name)
    #     u = upload_download_file_to_ceph(s,file_name)
    #     # # rseml = eml_from_recordset(q[1],env="prod")
    #     # # e = upload_eml_file_to_ceph(s,q[1],rseml)
    #     print(q[1], u)
    #     count += 1



    file_name = generate_files(record_query=queryFromShim({"geopoint":{"type":"exists"},"taxonomicstatus":"accepted","taxonid":{"type":"exists"},"flags":"gbif_taxon_corrected","kingdom":"plantae","geopoint":{"type":"geo_bounding_box","top_left":{"lat":89,"lon":-179},"bottom_right":{"lat":-89,"lon":-33}}})["query"], form="dwca-csv", filename="idigbio-plantae-w")
    u = upload_download_file_to_ceph(s, file_name)
    # file_name = generate_files(record_query=queryFromShim({"geopoint":{"type":"exists"},"taxonomicstatus":"accepted","taxonid":{"type":"exists"},"flags":"gbif_taxon_corrected","kingdom":"plantae","geopoint":{"type":"geo_bounding_box","top_left":{"lat":89,"lon":-179},"bottom_right":{"lat":12,"lon":-47}}})["query"], form="dwca-csv", filename="idigbio-plantae-na")
    # u = upload_download_file_to_ceph(s, file_name)
    # file_name = generate_files(record_query=queryFromShim({"geopoint":{"type":"exists"},"taxonomicstatus":"accepted","taxonid":{"type":"exists"},"flags":"gbif_taxon_corrected",})["query"], form="dwca-csv", filename="idigbio-geotaxon")
    # u = upload_download_file_to_ceph(s, file_name)
    # file_name = generate_files(record_query=queryFromShim({})["query"], form="dwca-csv", filename="idigbio")
    # u = upload_download_file_to_ceph(s, file_name)

if __name__ == '__main__':
    main()
