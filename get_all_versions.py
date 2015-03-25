import sys
import json

from riak import RiakClient, RiakObject, RiakBucket
from postgres_backend.db import PostgresDB
from helpers.etags import calcEtag

riak = RiakClient(nodes=[
	{"host": "c18node2.acis.ufl.edu"},
	{"host": "c18node6.acis.ufl.edu"},
	{"host": "c18node10.acis.ufl.edu"},
	{"host": "c18node12.acis.ufl.edu"},
	{"host": "c18node14.acis.ufl.edu"}
])

db = PostgresDB()

def de_setter(etag_set):
    for e in etag_set:
        yield {"etag": e}

for t in reversed(["record","mediarecord","recordset","publisher"]):
    bucket = riak.bucket(t + "_catalog")
    data_bucket = riak.bucket(t)
    etag_set = set()
    for r in db.get_type_list(t,limit=None):
        ro = bucket.get(r["uuid"])
        for i,e in enumerate(ro.data["idigbio:etags"]):
            try:
                do = data_bucket.get(r["uuid"] + "-" + e)
                retag = calcEtag(do.data["idigbio:data"])
                dm = do.data["idigbio:dateModified"]
                print t, r["uuid"], r["parent"], i, e, retag, dm
            except KeyboardInterrupt:
                sys.exit()
            except:
                print t, r["uuid"], r["parent"], i, e, None, None
