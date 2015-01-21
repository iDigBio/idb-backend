import uuid
import json
import itertools
import datetime
import copy

from postgres_backend import pg, DictCursor
from helpers.index_helper import index_record
from helpers.conversions  import grabAll
from corrections.record_corrector import RecordCorrector
from elasticsearch_backend.indexer import ElasticSearchIndexer
from config import config

def type_yield(ei,rc,typ):
    cursor = pg.cursor(str(uuid.uuid4()),cursor_factory=DictCursor)

    cursor.execute("select id,etag,data::json from cache where type=%s", (typ,))

    start_time = datetime.datetime.now()
    count = 0.0
    for r in cursor:
        count += 1.0
        yield index_record(ei,rc,typ,r,do_index=False)

        if count % 10000 == 0:
            print typ, count, datetime.datetime.now() - start_time, count/(datetime.datetime.now() - start_time).total_seconds()

    print typ, count, datetime.datetime.now() - start_time, count/(datetime.datetime.now() - start_time).total_seconds()

def main():
    ei = ElasticSearchIndexer(config["elasticsearch"]["indexname"],config["elasticsearch"]["types"],serverlist=config["elasticsearch"]["servers"])

    rc = RecordCorrector()

    for typ in config["elasticsearch"]["types"]:
        for ok, item in ei.bulk_index(type_yield(ei,rc,typ)):
            pass

if __name__ == '__main__':
    main()