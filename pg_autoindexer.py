from postgres_backend import pg, DictCursor
from redis_backend import redist

import uuid
import json
import itertools
import datetime
import copy

from idigbio.lib.indexer.conversions import grabAll
from idigbio.lib.indexer.elasticsearch import ElasticSearchIndexer

types = ["publishers","recordsets","records","mediarecords"]
indexname = "2.0.0"

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

    p = redist.pubsub()

    p.psubscribe("pg_incremental_indexer_*")

    cursor = pg.cursor(cursor_factory=DictCursor)

    while True:
        message = p.get_message()
        if message is not None:
            typ = message["channel"][len("pg_incremental_indexer_"):]
            e = redist.spop("pg_incremental_indexer_" + t + "_queue")
            if e is not None:
                cursor.execute("select id,etag,data::json from cache where type=%s and id=%s", (typ,e))

                for r in cursor:
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
            else:
                time.sleep(1)
        else:
            time.sleep(1)

if __name__ == '__main__':
    main()