import uuid
import json
import itertools
import datetime
import copy
import time
import traceback

from postgres_backend import pg, DictCursor
from redis_backend import redist
from redis_backend.queue import RedisQueue
from corrections.record_corrector import RecordCorrector
from helpers.index_helper import index_record
from helpers.conversions  import grabAll
from elasticsearch_backend.indexer import ElasticSearchIndexer
from config import config

def index_item(cursor,typ,e,rc,q,ei):
    cursor.execute("select id,etag,data::json from cache where type=%s and id=%s", (typ,e))

    for r in cursor:
        try:
            return index_record(ei,rc,typ,r, do_index=False)
        except:
            q.add(typ,e)
            print typ, e
            traceback.print_exc()

def queue_generator(ei,rc,q,cursor):
    for typ in config["elasticsearch"]["types"]:
        print "Drain", typ
        for typ, e in q.drain(typ):
            yield index_item(cursor,typ,e,rc,q,ei)

    print "Listen"
    for typ, e in q.listen():
        yield index_item(cursor,typ,e,rc,q,ei)

def main():
    ei = ElasticSearchIndexer(config["elasticsearch"]["indexname"],config["elasticsearch"]["types"],serverlist=config["elasticsearch"]["servers"])

    rc = RecordCorrector()

    q = RedisQueue(queue_prefix="pg_incremental_indexer_")

    cursor = pg.cursor(cursor_factory=DictCursor)

    for ok, item in ei.bulk_index(queue_generator(ei,rc,q,cursor)):
        pass

if __name__ == '__main__':
    main()