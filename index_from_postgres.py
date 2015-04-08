import uuid
import json
import itertools
import datetime
import copy
import sys

from postgres_backend import pg, DictCursor
from helpers.index_helper import index_record
from helpers.conversions  import grabAll
from corrections.record_corrector import RecordCorrector
from elasticsearch_backend.indexer import ElasticSearchIndexer
from config import config

import elasticsearch.helpers

def type_yield(ei,rc,typ):
    cursor = pg.cursor(str(uuid.uuid4()),cursor_factory=DictCursor)

    cursor.execute("select * from idigbio_uuids_data where type=%s", ("".join(typ[:-1]),))

    start_time = datetime.datetime.now()
    count = 0.0
    for r in cursor:
        count += 1.0
        yield index_record(ei,rc,typ,r,do_index=False)

        if count % 10000 == 0:
            print typ, count, datetime.datetime.now() - start_time, count/(datetime.datetime.now() - start_time).total_seconds()

    print typ, count, datetime.datetime.now() - start_time, count/(datetime.datetime.now() - start_time).total_seconds()

def type_yield_resume(ei,rc,typ):
    es_ids = {}

    print "Building Resume Cache", typ
    q = {
        "index": ei.indexName,
        "doc_type": typ,
        "_source": ["etag"],
        "size": 10000,
        "scroll": "10m"
    }
    cache_count = 0.0
    for r in elasticsearch.helpers.scan(ei.es,**q):
        cache_count += 1.0
        k = r["_id"]
        etag = None
        if "etag" in r["_source"]:
            etag = r["_source"]["etag"]
        else:
            etag = ""

        es_ids[k] = etag

        if cache_count % 10000 == 0:
            print cache_count

    print "Indexing", typ
    cursor = pg.cursor(str(uuid.uuid4()),cursor_factory=DictCursor)

    cursor.execute("select * from idigbio_uuids_data where type=%s", (typ,))

    start_time = datetime.datetime.now()
    count = 0.0
    for r in cursor:
        count += 1.0
        if r["id"] in es_ids and es_ids[r["id"]] == r["etag"]:
            continue

        yield index_record(ei,rc,typ,r,do_index=False)

        if count % 10000 == 0:
            print typ, count, datetime.datetime.now() - start_time, count/(datetime.datetime.now() - start_time).total_seconds()

    print typ, count, datetime.datetime.now() - start_time, count/(datetime.datetime.now() - start_time).total_seconds()


def main():
    # sl = config["elasticsearch"]["servers"]
    sl = [
        "c17node52.acis.ufl.edu",
        "c17node53.acis.ufl.edu",
        "c17node54.acis.ufl.edu",
        "c17node55.acis.ufl.edu",
        "c17node56.acis.ufl.edu"
    ]
    ei = ElasticSearchIndexer(config["elasticsearch"]["indexname"],config["elasticsearch"]["types"],serverlist=sl)

    rc = RecordCorrector()

    if len(sys.argv) > 1 and sys.argv[1] == "resume":
        for typ in ei.types:
            for ok, item in ei.bulk_index(type_yield_resume(ei,rc,typ)):
                pass
    else:
        for typ in ei.types:
            for ok, item in ei.bulk_index(type_yield(ei,rc,typ)):
                pass

if __name__ == '__main__':
    main()
