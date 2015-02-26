from postgres_backend import pg as cache_pg, pg_conf, DictCursor, psycopg2
from redis_backend.queue import RedisQueue
from config import config

from elasticsearch import Elasticsearch
import elasticsearch.helpers

import uuid
import copy
import gc
import traceback

import logging

FORMAT = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
logging.basicConfig(format=FORMAT)

api_pg_conf = copy.deepcopy(pg_conf)
api_pg_conf["user"] = "idigbio-api"
api_pg_conf["database"] = "idb-api-prod"

api_pg = psycopg2.connect(**api_pg_conf)

es = Elasticsearch(config["elasticsearch"]["servers"], sniff_on_start=True, sniff_on_connection_fail=True,retry_on_timeout=True, max_retries=3)

types = config["elasticsearch"]["types"]
#types = ["publishers","recordsets"]

cache_q = RedisQueue("cacher_")
index_q = RedisQueue("pg_incremental_indexer_")

def main():
    for t in types:
        print "TYPE:", t

        cache_cursor = cache_pg.cursor(str(uuid.uuid4()),cursor_factory=DictCursor)

        cache_cursor.execute("select id, etag from cache where type = %s", (t,))

        cache_ids = {}
        for r in cache_cursor:
            cache_ids[r["id"]] = r["etag"]

        cache_ids_count = len(cache_ids.viewkeys())

        # START Phase 1 - API vs Cache

        api_cursor = api_pg.cursor(str(uuid.uuid4()),cursor_factory=DictCursor)

        api_cursor.execute("select id, etag from idigbio_uuids where type = %s and deleted=false", (t[:-1],))

        api_id_count = 0
        intersect_count = 0
        both_but_differ = set()
        api_ids_only = set()
        cache_ids_only = set()
        cache_visited = set()

        for r in api_cursor:
            api_id_count += 1
            k = r["id"]
            etag = r["etag"]
            if k in cache_ids:
                if etag == cache_ids[k]:
                    intersect_count += 1
                    cache_visited.add(k)
                else:
                    both_but_differ.add(k)
                    cache_visited.add(k)
            else:
                api_ids_only.add(k)

        cache_ids_only = cache_ids.viewkeys() - cache_visited

        print "API       :", api_id_count
        print "Cache     :", cache_ids_count
        print "Equal     :", intersect_count
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
        del both_but_differ
        del api_ids_only
        del cache_ids_only
        del cache_visited

        gc.collect()
        # START Phase 2 - Cache Vs ES

        intersect_count = 0
        es_ids_count = 0
        both_but_differ = set()
        es_ids_only = set()

        q = {
            "index": "idigbio-" + config["elasticsearch"]["indexname"],
            "doc_type": t,
            "_source": ["etag"],
            "size": 10000,
            "scroll": "10m"
        }
        for r in elasticsearch.helpers.scan(es,**q):
            es_ids_count += 1
            k = r["_id"]
            etag = None
            if "etag" in r["_source"]:
                etag = r["_source"]["etag"]
            else:
                etag = ""
        
            if k in cache_ids:
                if cache_ids[k] == etag:
                    intersect_count += 1
                    del cache_ids[k]
                else:
                    both_but_differ.add(k)
                    del cache_ids[k]                
            else:
                es_ids_only.add(k)

        cache_ids_only = cache_ids.viewkeys()

        print "Cache     :", cache_ids_count
        print "Search    :", es_ids_count
        print "Equal     :", intersect_count
        print "Different :", len(both_but_differ)
        print "Cache Only:", len(cache_ids_only)
        print "ES Only   :", len(es_ids_only)
        print

        for k in both_but_differ | cache_ids_only:
            index_q.add(t,k)

        for k in es_ids_only:
            try:
                es.delete(index="idigbio",doc_type=t,id=k)
            except:
                traceback.print_exc()

        # END Phase 1
        del cache_ids
        del both_but_differ
        del es_ids_only
        del cache_ids_only

        gc.collect()
        # END Phase 2 - Cache Vs ES

if __name__ == '__main__':
    main()