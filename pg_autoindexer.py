from postgres_backend import pg, DictCursor
from redis_backend import redist
from redis_backend.queue import RedisQueue

import uuid
import json
import itertools
import datetime
import copy
import time
import traceback

from idigbio.lib.indexer.conversions import grabAll
from idigbio.lib.indexer.elasticsearch import ElasticSearchIndexer

types = ["publishers","recordsets","records","mediarecords"]
indexname = "2.0.0"

def index_item(cursor,typ,e,corrections,q,ei):
    cursor.execute("select id,etag,data::json from cache where type=%s and id=%s", (typ,e))

    for r in cursor:
        try:
            d = copy.deepcopy(r["data"]["idigbio:data"])

            for c in corrections:
                l = []
                for k in c:
                    if k in d:
                        l.append(d[k])
                    else:
                        break
                else: # if we got to the end of the for without breaking
                    uv = tuple(l)
                    if uv in corrections[c]:
                        d.update(corrections[c][uv])
            d.update(r["data"])
            del d["idigbio:data"]

            i =  ei.prepForEs(typ,grabAll(typ,d))
            i["data"] = r["data"]

            ei.index(typ,i)
        except:
            q.add(typ,e)
            print typ, e
            traceback.print_exc()
def main():
    ei = ElasticSearchIndexer(indexname,types,serverlist=[
        "http://c17node52.acis.ufl.edu:9200",
        "http://c17node53.acis.ufl.edu:9200",
        "http://c17node54.acis.ufl.edu:9200",
        "http://c17node55.acis.ufl.edu:9200",
        "http://c17node56.acis.ufl.edu:9200"
    ])

    cursor = pg.cursor(str(uuid.uuid4()),cursor_factory=DictCursor)

    cursor.execute("select k::json,v::json from corrections")

    corrections = {}
    for r in cursor:
        uk = tuple(r["k"]["idigbio:data"].keys())
        uv = tuple([r["k"]["idigbio:data"][k] for k in uk])
        if uk not in corrections:
            corrections[uk] = {}

        corrections[uk][uv] = r["v"]

    print corrections.keys()

    q = RedisQueue(queue_prefix="pg_incremental_indexer_")

    cursor = pg.cursor(cursor_factory=DictCursor)

    for typ in types:
        print "Drain", typ
        for typ, e in q.drain(typ):
            index_item(cursor,typ,e,corrections,q,ei)
    for typ,e in q.listen():
        index_item(cursor,typ,e,corrections,q,ei)

if __name__ == '__main__':
    main()