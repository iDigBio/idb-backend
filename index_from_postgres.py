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
    pg_typ = "".join(typ[:-1])

    cursor = pg.cursor(str(uuid.uuid4()),cursor_factory=DictCursor)

    cursor.execute("select * from idigbio_uuids_data where type=%s", (pg_typ,))

    start_time = datetime.datetime.now()
    count = 0.0
    for r in cursor:
        count += 1.0
        yield index_record(ei,rc,typ,r,do_index=False)

        if count % 10000 == 0:
            print typ, count, datetime.datetime.now() - start_time, count/(datetime.datetime.now() - start_time).total_seconds()

    print typ, count, datetime.datetime.now() - start_time, count/(datetime.datetime.now() - start_time).total_seconds()

def type_yield_modified(ei,rc,typ):
    pg_typ = "".join(typ[:-1])
    es_ids = {}

    q = {
        "index": ei.indexName,
        "size": 0,
        "doc_type": typ,
        "body": {
            "aggs": {
                "mm": {
                    "max": {
                        "field": "datemodified"
                    }
                }
            }
        }
    }

    o = ei.es.search(**q)

    after = datetime.datetime.utcfromtimestamp(o["aggregations"]["mm"]["value"]/1000)

    print "Indexing", typ, "after", after.isoformat()
    cursor = pg.cursor(str(uuid.uuid4()),cursor_factory=DictCursor)

    # Note, a subtle distinction: The below query will index every _version_ of every record modified since the date
    # it is thus imperative that the records are process in ascending modified order.
    # in practice, this is unlikely to index more than one record in a single run, but it is possible.
    cursor.execute("""SELECT
            uuids.id as uuid,
            type,
            deleted,
            data_etag as etag,
            version,
            modified,
            parent,
            recordids,
            siblings,
            data,
            riak_etag
        FROM uuids_data
        LEFT JOIN uuids
        ON uuids.id = uuids_data.uuids_id
        LEFT JOIN data
        ON data.etag = uuids_data.data_etag
        LEFT JOIN LATERAL (
            SELECT uuids_id, array_agg(identifier) as recordids
            FROM uuids_identifier
            WHERE uuids_id=uuids.id
            GROUP BY uuids_id
        ) as ids
        ON ids.uuids_id=uuids.id
            LEFT JOIN LATERAL (
            SELECT subject, json_object_agg(rel,array_agg) as siblings
            FROM (
                SELECT subject, rel, array_agg(object)
                FROM (
                    SELECT
                        r1 as subject,
                        type as rel,
                        r2 as object
                    FROM (
                        SELECT r1,r2
                        FROM uuids_siblings
                        UNION
                        SELECT r2,r1
                        FROM uuids_siblings
                    ) as rel_union
                    JOIN uuids
                    ON r2=id
                    WHERE uuids.deleted = false
                ) as rel_table
                WHERE subject=uuids.id
                GROUP BY subject, rel
            ) as rels
            GROUP BY subject
        ) as sibs
        ON sibs.subject=uuids.id
        WHERE type=%s and modified>%s
        ORDER BY modified ASC;
        """, (pg_typ,after))

    start_time = datetime.datetime.now()
    count = 0.0
    for r in cursor:
        count += 1.0

        yield index_record(ei,rc,typ,r,do_index=False)

        if count % 10000 == 0:
            print typ, count, datetime.datetime.now() - start_time, count/(datetime.datetime.now() - start_time).total_seconds()

    print typ, count, datetime.datetime.now() - start_time, count/(datetime.datetime.now() - start_time).total_seconds()


def type_yield_resume(ei,rc,typ):
    pg_typ = "".join(typ[:-1])
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

    cursor.execute("select * from idigbio_uuids_data where type=%s", (pg_typ,))

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
    elif len(sys.argv) > 1 and sys.argv[1] == "incremental":
        for typ in ei.types:
            for ok, item in ei.bulk_index(type_yield_modified(ei,rc,typ)):
                pass
    else:
        for typ in ei.types:
            for ok, item in ei.bulk_index(type_yield(ei,rc,typ)):
                pass

if __name__ == '__main__':
    main()
