from postgres_backend import pg as cache_pg, pg_conf, DictCursor, psycopg2
from redis_backend.queue import RedisQueue

from elasticsearch import Elasticsearch
import elasticsearch.helpers

import uuid
import copy
import gc

api_pg_conf = copy.deepcopy(pg_conf)
api_pg_conf["user"] = "idigbio-api"
api_pg_conf["database"] = "idb-api-prod"

api_pg = psycopg2.connect(**api_pg_conf)

es = Elasticsearch([
        "http://c17node52.acis.ufl.edu:9200",
        "http://c17node53.acis.ufl.edu:9200",
        "http://c17node54.acis.ufl.edu:9200",
        "http://c17node55.acis.ufl.edu:9200",
        "http://c17node56.acis.ufl.edu:9200"
    ], sniff_on_start=True, sniff_on_connection_fail=True)

types = ["publishers","recordsets","mediarecords","records"]
#types = ["publishers","recordsets"]

cache_q = RedisQueue("cacher_")
index_q = RedisQueue("pg_incremental_indexer_")

for t in types:
    print "TYPE:", t

    # START Phase 1 - API vs Cache
    api_cursor = api_pg.cursor(str(uuid.uuid4()),cursor_factory=DictCursor)

    api_cursor.execute("select id, etag from idigbio_uuids where type = %s and deleted=false", (t[:-1],))

    api_ids = {}
    for r in api_cursor:
        api_ids[r["id"]] = r["etag"]

    cache_cursor = cache_pg.cursor(str(uuid.uuid4()),cursor_factory=DictCursor)

    cache_cursor.execute("select id, etag from cache where type = %s", (t,))

    cache_ids = {}
    for r in cache_cursor:
        cache_ids[r["id"]] = r["etag"]


    print "API  :", len(api_ids.viewkeys())
    print "Cache:", len(cache_ids.viewkeys())

    both_but_differ = set()
    intersect = set()
    api_ids_only = set()

    for k in api_ids.keys():
        if k in cache_ids:
            if api_ids[k] == cache_ids[k]:
                intersect.add(k)
                del cache_ids[k]
                del api_ids[k]
            else:
                both_but_differ.add(k)
                del cache_ids[k]
                del api_ids[k]                
        else:
            api_ids_only.add(k)

    cache_ids_only = cache_ids.viewkeys()    

    print "Equal     :", len(intersect)
    print "Different :", len(both_but_differ)
    print "API Only  :", len(api_ids_only)
    print "Cache Only:", len(cache_ids_only)
    print

    for k in both_but_differ | api_ids_only:
        cache_q.add(t,k)

    cache_cursor = cache_pg.cursor()
    cache_cursor.execute("BEGIN")
    for k in cache_ids_only:
        cache_cursor.execute("DELETE FROM cache WHERE id = %s", (k,))
    cache_pg.commit()

    # END Phase 1
    api_ids = None
    cache_ids = None
    both_but_differ = None
    intersect = None
    api_ids_only = None
    cache_ids_only = None

    gc.collect()
    # START Phase 2 - Cache Vs ES

    cache_cursor = cache_pg.cursor(str(uuid.uuid4()),cursor_factory=DictCursor)

    cache_cursor.execute("select id, etag from cache where type = %s", (t,))

    cache_ids = {}
    for r in cache_cursor:
        cache_ids[r["id"]] = r["etag"]

    q = {
        "index": "idigbio",
        "doc_type": t,
        "_source": ["etag"],
        "size": 100000
    }
    es_ids = {}
    for r in elasticsearch.helpers.scan(es,**q):
        if "etag" in r["_source"]:
            es_ids[r["_id"]] = r["_source"]["etag"]
        else:
            es_ids[r["_id"]] = ""
    
    print "Cache :", len(cache_ids.viewkeys())
    print "Search:", len(es_ids.viewkeys())

    both_but_differ = set()
    intersect = set()
    cache_ids_only = set()

    for k in cache_ids.keys():
        if k in es_ids:
            if cache_ids[k] == es_ids[k]:
                intersect.add(k)
                del es_ids[k]
                del cache_ids[k]
            else:
                both_but_differ.add(k)
                del es_ids[k]
                del cache_ids[k]                
        else:
            cache_ids_only.add(k)

    es_ids_only = es_ids.viewkeys()

    print "Equal     :", len(intersect)
    print "Different :", len(both_but_differ)
    print "Cache Only  :", len(cache_ids_only)
    print "ES Only:", len(es_ids_only)
    print

    for k in both_but_differ | cache_ids_only:
        index_q.add(t,k)

    for k in es_ids_only:
        es.delete(index="idigbio",doc_type=t,id=k)