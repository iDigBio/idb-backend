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
    import copy
except ImportError as e:
    print ("IMPORT ERROR (This exception is likely caused by a missing module): '{0}'".format(e))
    raise SystemExit


MEDIA_QUERY_BASE={
     "query":
     {"bool":
       {"must":[
        { "wildcard":
            {
                "accessuri": "http://media.idigbio.org/lookup/images/"
            }
        },
        { "term":
            {
                "hasSpecimen": True
            }
        }
    ]}}}

# 100k is max result size (count/hits) configured in our Elasticsearch
#SIZE=100000
SIZE=1

#ES_SERVERS=["c20node1.acis.ufl.edu:9200"]
# Accessing via localhost is possible after port forwarding:
# $ ssh -nNT -L 9200:c20node1.acis.ufl.edu:9200  USER@c18node4.acis.ufl.edu
ES_SERVERS=["localhost:9200"]


es = get_connection(hosts=ES_SERVERS)

# Verify connectivity to cluster.  The logic here is correct (compared to indexer), 
# es.ping() returns true/false in its basic usage and exception is only for _exceptional_ issues.
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


    # The Appliance (media-only) recordset media records contain:
    #
    #    "hasSpecimen": false,
    #
    # and have no reference to the specimen record.
    #
    # Similarly, the database does not seem to have a sibling relationship.
    # 
    #    idb_api_prod=> select * from uuids_siblings where r1 = '000076b5-47ad-4fb1-9348-60106c8403ff';
    #    id | r1 | r2 
    #    ----+----+----
    #    (0 rows)
    #
    #    idb_api_prod=> select * from uuids_siblings where r2 = '000076b5-47ad-4fb1-9348-60106c8403ff';
    #    id | r1 | r2 
    #    ----+----+----
    #    (0 rows)
    # 
    # So this code cannot find specimen records for those.

# Problem space is broken up by first letter of etag to get around result count limits.
for etag_prefix in ["0", "1", "2", "3", "4", "5", "6", "7", "8", "9", "a", "b", "c", "d", "e", "f"]:
    QUERY = copy.deepcopy(MEDIA_QUERY_BASE)
    QUERY["query"]["bool"]["must"][0]["wildcard"]["accessuri"] = QUERY["query"]["bool"]["must"][0]["wildcard"]["accessuri"] + etag_prefix + "*"
    #print (QUERY)
    resp = es.search(index="idigbio", _source_include=["recordsets"], size=SIZE, body=QUERY)

    for each in resp["hits"]["hits"]:
        print(each)
        #recordsets_referencing_media.add(each["recordsets"][0])

print (recordsets_referencing_media)