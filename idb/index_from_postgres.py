MULTIPROCESS = False

if MULTIPROCESS:
    from multiprocessing import Pool    
else:
    # import gevent.monkey
    # gevent.monkey.patch_all()
    from gevent.pool import Pool


import uuid
import json
import itertools
import functools
import datetime
import time
import copy
import sys
import gc
import os

from postgres_backend import pg, DictCursor
from helpers.index_helper import index_record
from corrections.record_corrector import RecordCorrector
from elasticsearch_backend.indexer import ElasticSearchIndexer
from config import config

import elasticsearch.helpers

last_afters = {}

# Maximum number of seconds to sleep
MAX_SLEEP = 600
MIN_SLEEP = 120

def type_yield(ei,rc,typ,yield_record=False):
    # drop the trailing s
    pg_typ = "".join(typ[:-1])

    cursor = pg.cursor(str(uuid.uuid4()),cursor_factory=DictCursor)

    cursor.execute("select * from idigbio_uuids_data where type=%s and deleted=false", (pg_typ,))

    start_time = datetime.datetime.now()
    count = 0.0
    for r in cursor:
        count += 1.0
        if yield_record:
            yield r
        else:
            yield index_record(ei,rc,typ,r,do_index=False)

        if count % 10000 == 0:
            print typ, count, datetime.datetime.now() - start_time, count/(datetime.datetime.now() - start_time).total_seconds()

    print typ, count, datetime.datetime.now() - start_time, count/(datetime.datetime.now() - start_time).total_seconds()

def type_yield_modified(ei,rc,typ,yield_record=False):
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

    if typ in last_afters:
        if after == last_afters[typ]:
            print typ, "after value", after, "same as last run, skipping"
            return
        else:
            last_afters[typ] = after
    else:
        last_afters[typ] = after

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
        WHERE type=%s and modified>%s and deleted=false
        ORDER BY modified ASC;
        """, (pg_typ,after))

    start_time = datetime.datetime.now()
    count = 0.0
    for r in cursor:
        count += 1.0

        if yield_record:
            yield r
        else:
            yield index_record(ei,rc,typ,r,do_index=False)

        if count % 10000 == 0:
            print typ, count, datetime.datetime.now() - start_time, count/(datetime.datetime.now() - start_time).total_seconds()

    print typ, count, datetime.datetime.now() - start_time, count/(datetime.datetime.now() - start_time).total_seconds()


def type_yield_resume(ei,rc,typ,also_delete=False,yield_record=False):
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

    #     if cache_count % 10000 == 0:
    #         print cache_count

    # print cache_count

    print "Indexing", typ
    cursor = pg.cursor(str(uuid.uuid4()),cursor_factory=DictCursor)

    cursor.execute("select * from idigbio_uuids_data where type=%s and deleted=false", (pg_typ,))

    start_time = datetime.datetime.now()
    count = 0.0
    for r in cursor:
        count += 1.0
        if r["uuid"] in es_ids:
            etag = es_ids[r["uuid"]]
            del es_ids[r["uuid"]]
            if etag == r["etag"]:
                continue

        if yield_record:
            yield r
        else:
            yield index_record(ei,rc,typ,r,do_index=False)

        if count % 10000 == 0:
            print typ, count, datetime.datetime.now() - start_time, count/(datetime.datetime.now() - start_time).total_seconds()

    print typ, count, datetime.datetime.now() - start_time, count/(datetime.datetime.now() - start_time).total_seconds()

    if also_delete and len(es_ids) > 0:
        print "Deleting", len(es_ids), "extra", typ
        for r in es_ids:
            ei.es.delete_by_query(**{
                "index": ei.indexName,
                "doc_type": typ,
                "body": {
                    "query": {
                        "filtered": {
                            "filter": {
                                "term":{
                                    "uuid": r
                                }
                            }
                        }
                    }
                }
            })

def delete(ei, no_index=False):
    print "Running deletes"
    cursor = pg.cursor(str(uuid.uuid4()),cursor_factory=DictCursor)

    count = 0
    cursor.execute("SELECT id,type FROM uuids WHERE deleted=true")
    for r in cursor:
        count += 1
        if not no_index:
            ei.es.delete_by_query(**{
                "index": ei.indexName,
                "doc_type": r["type"],
                "body": {
                    "query": {
                        "filtered": {
                            "filter": {
                                "term":{
                                    "uuid": r["id"]
                                }
                            }
                        }
                    }
                }
            })

        if count % 10000 == 0:
            print count

    print count
    try:
        ei.optimize()
    except:
        pass

def resume(ei, rc, also_delete=False, no_index=False):
    # Create a partial application to add in the keyword argument
    f = functools.partial(type_yield_resume,also_delete=also_delete)
    consume(ei, rc, f, no_index=no_index)

def full(ei, rc, no_index=False):
    consume(ei, rc, type_yield, no_index=no_index)

def incremental(ei,rc, no_index=False):
    consume(ei, rc, type_yield_modified, no_index=no_index)
    try:
        ei.optimize()
    except:
        pass

def consume(ei, rc, iter_func, no_index=False):
    p = Pool(10)
    for typ in ei.types:
        # Construct a version of index record that can be just called with the record
        index_func = functools.partial(index_record, ei, rc, typ, do_index=False)
        if no_index:
            for _ in p.imap(index_func,iter_func(ei, rc, typ, yield_record=True)):
                pass
        else:
            for ok, item in ei.bulk_index(p.imap(index_func,iter_func(ei, rc, typ, yield_record=True))):
                pass
        gc.collect()

def continuous_incremental(ei,rc, no_index=False):
    while True:
        t_start = datetime.datetime.now()
        print "Starting Incremental Run at", t_start.isoformat()
        incremental(ei,rc, no_index=no_index)
        t_end = datetime.datetime.now()
        print "Ending Incremental Run from", t_start.isoformat(), "at", t_end
        sleep_duration = max([MAX_SLEEP - (t_end - t_start).total_seconds(),MIN_SLEEP])
        print "Sleeping for", sleep_duration, "seconds"
        time.sleep(sleep_duration)

def main():
    import argparse

    parser = argparse.ArgumentParser(description='Index data from new database to elasticsearch')
    parser.add_argument('-i', '--incremental', dest='incremental', action='store_true', help='run incremental index')
    parser.add_argument('-f', '--full', dest='full', action='store_true', help='run full sync')
    parser.add_argument('-r', '--resume', dest='resume', action='store_true', help='resume a full sync (full + etag compare)')
    parser.add_argument('-c', '--continuous', dest='continuous', action='store_true', help='run incemental continously (implies -i)')
    parser.add_argument('-d', '--delete', dest='delete', action='store_true', help='delete records from index that are deleted in api')
    parser.add_argument('-k', '--check', dest='check', action='store_true', help='run a full check (delete + resume)')
    parser.add_argument('-n', '--noindex', dest='no_index', action='store_true', help="don't actually index records")
    parser.add_argument('-t', '--types', dest='types', nargs='+', type=str, default=config["elasticsearch"]["types"])

    args = parser.parse_args()

    if any(args.__dict__.values()):
        sl = config["elasticsearch"]["servers"]
        if os.environ["ENV"] == "beta":
            sl = [
                "c17node52.acis.ufl.edu",
                "c17node53.acis.ufl.edu",
                "c17node54.acis.ufl.edu",
                "c17node55.acis.ufl.edu",
                "c17node56.acis.ufl.edu"
            ]
        ei = ElasticSearchIndexer(config["elasticsearch"]["indexname"],args.types,serverlist=sl)

        rc = RecordCorrector()

        if args.continuous:
            continuous_incremental(ei,rc)

        elif args.incremental:
            incremental(ei,rc,no_index=args.no_index)
        elif args.resume:
            resume(ei,rc,no_index=args.no_index)
        elif args.full:
            full(ei,rc,no_index=args.no_index)
        elif args.delete:
            delete(ei,no_index=args.no_index)
        elif args.check:
            resume(ei,rc,also_delete=True,no_index=args.no_index)
        else:
            parser.print_help()
    else:
        parser.print_help()

if __name__ == '__main__':
    main()
