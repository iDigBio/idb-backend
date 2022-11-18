from __future__ import print_function

from idb.indexing.indexer import get_connection
try:
    # from idigbio.json_client import iDbApiJson
    import idb.indexing.indexer
    import elasticsearch
    import elasticsearch.helpers
    import requests
    import shutil
    import os
    import sys
    import time
    import argparse
    import json
except ImportError as e:
    print ("IMPORT ERROR (This exception is likely caused by a missing module): '{0}'".format(e))
    raise SystemExit


MEDIA_QUERY_WILDCARD_BASE={
     "query":
        { "wildcard":
            {
                "accessuri":"http://media.idigbio.org/lookup/images/*"
            }
        }
    }
SIZE=1
#ES_SERVERS=["c20node12.acis.ufl.edu:9200"]
ES_SERVERS=["localhost:9200"]


es = get_connection(hosts=ES_SERVERS)

# verify connectivity to cluster
try:
    if es.ping():
        print ("Connection to '{0}' succeeded.".format(ES_SERVERS[0]))
    else:
        print ("Connection failed to cluster. First cluster node: '{0}'".format(ES_SERVERS[0]))
        raise SystemExit
except:
    print ("Connection failed to cluster. First cluster node: '{0}'".format(ES_SERVERS[0]))
    raise SystemExit


recordsets_referencing_media = set()

#results = es.get(index: "idigbio", params=arg_source=recordsets)
resp = es.search(index="idigbio", _source=["recordsets"], size=SIZE, body=MEDIA_QUERY_WILDCARD_BASE)
print(resp)