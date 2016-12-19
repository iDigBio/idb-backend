from elasticsearch import Elasticsearch
import elasticsearch.helpers
import os
import sys
import json
import dateutil.parser
import datetime
import zipfile

es = Elasticsearch([
        "c18node2.acis.ufl.edu:9200",
        "c18node6.acis.ufl.edu:9200",
        "c18node10.acis.ufl.edu:9200",
        "c18node12.acis.ufl.edu:9200",
        "c18node14.acis.ufl.edu:9200"
    ], sniff_on_start=True, sniff_on_connection_fail=True)

incremental = datetime.datetime.now() - datetime.timedelta(1)
if len(sys.argv) == 2 :
    incremental = dateutil.parser.parse(sys.argv[1],ignoretz=True)

types = [
    ("api", lambda r: (r["_source"]["recordset_id"], r["_source"]["harvest_date"]), {
        "query" : {
            "filtered": {
                "filter": {
                    "range": {
                        "harvest_date": {
                            "gte": incremental.isoformat()
                        }
                    }
                }
            }
        }
    }),
    ("digest", lambda r: (r["_source"]["recordset_id"], r["_source"]["harvest_date"]), {
        "query": {
            "filtered": {
                "filter": {
                    "range": {
                        "harvest_date": {
                            "gte": incremental.isoformat()
                        }
                    }
                }
            }
        }
    }),
    ("search", lambda r: (r["_source"]["recordset"], r["_source"]["date"]), {
        "query": {
            "filtered": {
                "filter": {
                    "range": {
                        "date": {
                            "gte": incremental.isoformat()
                        }
                    }
                }
            }
        }
    }),
]

path_prefix = "stats_backup"

with zipfile.ZipFile(path_prefix+".zip","a",zipfile.ZIP_DEFLATED,True) as z:
    for t, file_ider,q_body in types:
        q = {
            "index": "stats",
            "doc_type": t,
            "size": 10000,
            "scroll": "1m",
            "query": q_body
        }
        print t
        count = 0
        skip = 0
        for r in elasticsearch.helpers.scan(es,**q):
            count += 1
            file_id = file_ider(r)
            # paths = [
            #     "{0}".format(path_prefix),
            #     "{0}/{1}".format(path_prefix,t),
            #     "{0}/{1}/{2}".format(path_prefix,t,file_id[0])
            # ]
            fn = "{0}/{1}/{2}/{3}.json.gz".format(path_prefix,t,file_id[0],file_id[1])
            # for p in paths:
            #     if not os.path.exists(p):
            #         os.mkdir(p)
            # if not os.path.exists(fn):
            #     with open(fn,"wb") as outf:
            #        json.dump(r["_source"],outf)
            try:
                z.getinfo(fn)
                skip += 1
            except:
                z.writestr(fn,json.dumps(r["_source"]))

            if count % 10000 == 0:
                print count,skip
        print count,skip