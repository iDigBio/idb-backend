from __future__ import division, absolute_import
from __future__ import print_function


MULTIPROCESS = False

if MULTIPROCESS:
    from multiprocessing import Pool
else:
    # import gevent.monkey
    # gevent.monkey.patch_all()
    from gevent.pool import Pool, Timeout


import uuid
import json
import functools
import datetime
import time
import gc
import os
import math
import signal


from idb.postgres_backend import apidbpool, DictCursor
from idb.helpers.index_helper import index_record
from idb.corrections.record_corrector import RecordCorrector
from idb.elasticsearch_backend.indexer import ElasticSearchIndexer
from idb.config import config
from idb.helpers.signals import signalcm, ignored
from idb.helpers.logging import idblogger, configure_app_log

import elasticsearch.helpers

log = idblogger.getChild('index')

last_afters = {}

# Maximum number of seconds to sleep
MAX_SLEEP = 600
MIN_SLEEP = 120


def rate_logger(prefix, iterator):
    count = 0
    start_time = datetime.datetime.now()
    rate = lambda: count / (datetime.datetime.now() - start_time).total_seconds()
    output = lambda: log.info("%s %s %.1f/s", prefix, count, rate())

    with signalcm(signal.SIGUSR1, lambda s,f: output()):
        for e in iterator:
            yield e
            count += 1
            if count % 10000 == 0:
                output()

        log.info("%s %s %.1f/s FINISHED in %s",
                 prefix, count, rate(), datetime.datetime.now() - start_time)


def type_yield(ei, rc, typ, yield_record=False):
    # drop the trailing s
    pg_typ = "".join(typ[:-1])

    with apidbpool.cursor(name=str(uuid.uuid4()), cursor_factory=DictCursor) as cursor:
        cursor.execute(
            "select * from idigbio_uuids_data where type=%s and deleted=false", (pg_typ,))

        for r in rate_logger(typ, cursor):
            if yield_record:
                yield r
            else:
                yield index_record(ei, rc, typ, r, do_index=False)


def type_yield_modified(ei, rc, typ, yield_record=False):
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

    after = datetime.datetime.utcfromtimestamp(
        math.ceil(o["aggregations"]["mm"]["value"] / 1000))

    # This code doesn't work, I would need to poll the database for this information.
    # I don't care whether ES has changed or not (although presumably it hasn't).
    # if typ in last_afters:
    #     if after == last_afters[typ]:
    #         log.info("%s after value %s same as last run, skipping", typ, after)
    #         return
    #     else:
    #         last_afters[typ] = after
    # else:
    #     last_afters[typ] = after
    log.info("Indexing %s after %s", typ, after.isoformat())

    # Note, a subtle distinction: The below query will index every
    # _version_ of every record modified since the date it is thus
    # imperative that the records are process in ascending modified
    # order.  in practice, this is unlikely to index more than one
    # record in a single run, but it is possible.
    sql = ("""SELECT
            uuids.id as uuid,
            type,
            deleted,
            data_etag as etag,
            version,
            modified,
            parent,
            recordids,
            siblings,
            uuids_data.id as vid,
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
        """, (pg_typ, after))

    with apidbpool.cursor(name=str(uuid.uuid4()), cursor_factory=DictCursor) as cursor:
        cursor.execute(*sql)
        for r in rate_logger(typ, cursor):
            if yield_record:
                yield r
            else:
                yield index_record(ei, rc, typ, r, do_index=False)


def type_yield_resume(ei, rc, typ, also_delete=False, yield_record=False):
    pg_typ = "".join(typ[:-1])
    es_ids = {}
    log.info("%s Building Resume Cache", typ)

    q = {
        "index": ei.indexName,
        "doc_type": typ,
        "_source": ["etag"],
        "size": 10000,
        "scroll": "10m"
    }
    cache_count = 0
    for r in elasticsearch.helpers.scan(ei.es, **q):
        cache_count += 1
        k = r["_id"]
        etag = None
        if "etag" in r["_source"]:
            etag = r["_source"]["etag"]
        else:
            etag = ""

        es_ids[k] = etag

    log.info("%s: Indexing", typ)

    sql = (
        "select * from idigbio_uuids_data where type=%s and deleted=false", (pg_typ,))
    with apidbpool.cursor(name=str(uuid.uuid4()), cursor_factory=DictCursor) as cursor:
        cursor.execute(*sql)

        for r in rate_logger(typ, cursor):
            if r["uuid"] in es_ids:
                etag = es_ids[r["uuid"]]
                del es_ids[r["uuid"]]
                if etag == r["etag"]:
                    continue

            if yield_record:
                yield r
            else:
                yield index_record(ei, rc, typ, r, do_index=False)

    if also_delete and len(es_ids) > 0:
        log.info("%s: Deleting %s extra", len(es_ids))
        for r in es_ids:
            ei.es.delete(**{
                "index": ei.indexName,
                "doc_type": typ,
                "id": r
            })

def queryIter(query, ei, rc, typ, yield_record=False):
    q = {
        "index": ei.indexName,
        "doc_type": typ,
        "_source": [],
        "size": 10000,
        "scroll": "10m"
    }

    for r in elasticsearch.helpers.scan(ei.es, query=query, **q):
        sql = ("SELECT * FROM idigbio_uuids_data WHERE uuid=%s and type=%s",
               (r["_id"], typ[:-1]))
        rec = apidbpool.fetchone(*sql, cursor_factory=DictCursor)
        if rec is not None:
            if yield_record:
                yield rec
            else:
                yield index_record(ei, rc, typ, rec, do_index=False)

def uuidsIter(uuid_l, ei, rc, typ, yield_record=False):
    for rid in uuid_l:
        sql = ("SELECT * FROM idigbio_uuids_data WHERE uuid=%s and type=%s",
               (rid.strip(), typ[:-1]))
        rec = apidbpool.fetchone(*sql, cursor_factory=DictCursor)
        if rec is not None:
            if yield_record:
                yield rec
            else:
                yield index_record(ei, rc, typ, rec, do_index=False)

def delete(ei, no_index=False):
    log.info("Running deletes")

    count = 0
    sql = "SELECT id,type FROM uuids WHERE deleted=true"
    for r in apidbpool.fetchiter(sql, name=str(uuid.uuid4()), cursor_factory=DictCursor):
        count += 1
        if not no_index:
            ei.es.delete(**{
                "index": ei.indexName,
                "doc_type": r["type"],
                "id": r["id"]
            })

        if count % 10000 == 0:
            log.info("%s", count)

    log.info("%s", count)
    try:
        ei.optimize()
    except:
        pass


def resume(ei, rc, also_delete=False, no_index=False):
    # Create a partial application to add in the keyword argument
    f = functools.partial(type_yield_resume, also_delete=also_delete)
    consume(ei, rc, f, no_index=no_index)


def full(ei, rc, no_index=False):
    consume(ei, rc, type_yield, no_index=no_index)


def incremental(ei, rc, no_index=False):
    consume(ei, rc, type_yield_modified, no_index=no_index)
    try:
        ei.optimize()
    except:
        pass

def query(ei, rc, query, no_index=False):
    f = functools.partial(queryIter, query)
    consume(ei, rc, f, no_index=no_index)
    try:
        ei.optimize()
    except:
        pass

def uuids(ei, rc, uuid_l, no_index=False):
    f = functools.partial(uuidsIter, uuid_l)
    consume(ei, rc, f, no_index=no_index)
    try:
        ei.optimize()
    except:
        pass

def consume(ei, rc, iter_func, no_index=False):
    p = Pool(10)
    for typ in ei.types:
        # Construct a version of index record that can be just called with the
        # record
        def index_func(rec):
            with Timeout(5) as timeout:
                real_index_func = functools.partial(
                    index_record, ei, rc, typ, do_index=False)
                resp = real_index_func(rec)
            return resp
        if no_index:
            for _ in p.imap(index_func, iter_func(ei, rc, typ, yield_record=True)):
                pass
        else:
            for ok, item in ei.bulk_index(p.imap(index_func, iter_func(ei, rc, typ, yield_record=True))):
                pass
        gc.collect()


def continuous_incremental(ei, rc, no_index=False):
    while True:
        t_start = datetime.datetime.now()
        log.info("Starting Incremental Run at %s", t_start.isoformat())
        incremental(ei, rc, no_index=no_index)
        t_end = datetime.datetime.now()
        log.info("Ending Incremental Run from %s at %s",
                 t_start.isoformat(), t_end)
        sleep_duration = max(
            [MAX_SLEEP - (t_end - t_start).total_seconds(), MIN_SLEEP])
        log.info("Sleeping for %s seconds", sleep_duration)
        time.sleep(sleep_duration)


def main():
    import argparse

    parser = argparse.ArgumentParser(
        description='Index data from new database to elasticsearch')
    parser.add_argument('-i', '--incremental', dest='incremental',
                        action='store_true', help='run incremental index')
    parser.add_argument(
        '-f', '--full', dest='full', action='store_true', help='run full sync')
    parser.add_argument('-r', '--resume', dest='resume',
                        action='store_true', help='resume a full sync (full + etag compare)')
    parser.add_argument('-c', '--continuous', dest='continuous',
                        action='store_true', help='run incemental continously (implies -i)')
    parser.add_argument('-d', '--delete', dest='delete', action='store_true',
                        help='delete records from index that are deleted in api')
    parser.add_argument('-k', '--check', dest='check',
                        action='store_true', help='run a full check (delete + resume)')
    parser.add_argument('-n', '--noindex', dest='no_index',
                        action='store_true', help="don't actually index records")
    parser.add_argument('-t', '--types', dest='types', nargs='+',
                        type=str, default=config["elasticsearch"]["types"])
    parser.add_argument('-q', '--query', dest='query',
                        type=str, default="{}")
    parser.add_argument('-u', '--uuid', dest='uuid', nargs='+',
                        type=str, default=[])
    parser.add_argument('--uuid-file', dest='uuid_file',
                        type=str, default=None)

    args = parser.parse_args()

    if any(args.__dict__.values()):
        sl = config["elasticsearch"]["servers"]
        indexname = config["elasticsearch"]["indexname"]
        if os.environ["ENV"] == "beta":
            indexname = "2.5.0"
            sl = [
                "c17node52.acis.ufl.edu",
                "c17node53.acis.ufl.edu",
                "c17node54.acis.ufl.edu",
                "c17node55.acis.ufl.edu",
                "c17node56.acis.ufl.edu"
            ]
        ei = ElasticSearchIndexer(indexname, args.types, serverlist=sl)

        rc = RecordCorrector()

        if args.continuous:
            continuous_incremental(ei, rc)
        elif args.incremental:
            incremental(ei, rc, no_index=args.no_index)
        elif args.query != "{}":
            q = json.loads(args.query)
            query(ei, rc, q)
        elif args.uuid_file is not None:
            with open(args.uuid_file,"rb") as uf:
                uuids(ei,rc,uf.readlines())
        elif len(args.uuid) > 0:
            uuids(ei,rc,args.uuid)
        elif args.resume:
            resume(ei, rc, no_index=args.no_index)
        elif args.full:
            full(ei, rc, no_index=args.no_index)
        elif args.delete:
            delete(ei, no_index=args.no_index)
        elif args.check:
            resume(ei, rc, also_delete=True, no_index=args.no_index)
        else:
            parser.print_help()
    else:
        parser.print_help()

if __name__ == '__main__':
    configure_app_log(1)
    with ignored(signal.SIGUSR1):
        main()
