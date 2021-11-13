from __future__ import division, absolute_import
from __future__ import print_function

import itertools
import functools
import datetime
import time
import gc
import math
import signal
import logging

from idb.postgres_backend import apidbpool, DictCursor
from .index_helper import index_record
from idb.helpers.signals import signalcm
from idb.helpers.logging import idblogger, configure
from idb.postgres_backend.db import tombstone_etag

import elasticsearch.helpers

logger = idblogger.getChild('index_from_postgres')
configure(logger=logger, stderr_level=logging.INFO)

last_afters = {}

# Maximum number of seconds to sleep
MAX_SLEEP = 600
MIN_SLEEP = 120


def rate_logger(prefix, iterator, every=10000):
    count = 0
    start_time = datetime.datetime.now()
    rate = lambda: count / (datetime.datetime.now() - start_time).total_seconds()
    output = lambda: logger.info("%s %s %.1f/s", prefix, count, rate())

    with signalcm(signal.SIGUSR1, lambda s, f: output()):
        for e in iterator:
            yield e
            count += 1
            if count % every == 0:
                output()

        logger.info("%s %s %.1f/s FINISHED in %s",
                    prefix, count, rate(), datetime.datetime.now() - start_time)


def type_yield(ei, rc, typ, yield_record=False):
    # drop the trailing s
    pg_typ = "".join(typ[:-1])
    logger.info("Fetching rows for: %s", typ)
    sql = "SELECT * FROM idigbio_uuids_data WHERE type=%s AND deleted=false"
    results = apidbpool.fetchiter(sql, (pg_typ,),
                                  named=True, cursor_factory=DictCursor)
    for r in rate_logger(typ, results):
        if yield_record:
            yield r
        else:
            yield index_record(ei, rc, typ, r, do_index=False)


def type_yield_modified(ei, rc, typ, yield_record=False):
    pg_typ = "".join(typ[:-1])

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

    logger.info("Indexing %s after %s", typ, after.isoformat())

    # Note, a subtle distinction: The below query will index every
    # _version_ of every record modified since the date it is thus
    # imperative that the records are process in ascending modified
    # order.  in practice, this is unlikely to index more than one
    # record in a single run, but it is possible.
    sql = """SELECT
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
            data
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
            SELECT count(*) AS annotation_count
            FROM annotations
            WHERE uuids_id = uuids.id
        ) AS ac ON TRUE
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
        """

    results = apidbpool.fetchiter(
        sql, (pg_typ, after), named=True, cursor_factory=DictCursor)

    for r in rate_logger(typ, results):
        if yield_record:
            yield r
        else:
            yield index_record(ei, rc, typ, r, do_index=False)


def get_resume_cache(ei, typ):
    es_ids = {}
    logger.info("%s Building Resume Cache", typ)
    q = {
        "index": ei.indexName,
        "doc_type": typ,
        "_source": ["etag"],
        "size": 10000,
        "scroll": "10m"
    }
    cache_count = 0
    for r in rate_logger(typ + " resumecache",
                         elasticsearch.helpers.scan(ei.es, **q),
                         every=100000):
        cache_count += 1
        k = r["_id"]
        etag = r["_source"].get("etag", "")
        es_ids[k] = etag
    return es_ids


def type_yield_resume(ei, rc, typ, also_delete=False, yield_record=False):
    es_ids = get_resume_cache(ei, typ)
    logger.info("%s: Indexing", typ)
    pg_typ = "".join(typ[:-1])
    sql = "SELECT * FROM idigbio_uuids_data WHERE type=%s"
    if not also_delete:
        sql += " AND deleted=false"
    results = apidbpool.fetchiter(
        sql, (pg_typ,), named=True, cursor_factory=DictCursor)
    for r in rate_logger(typ + " indexing", results):
        es_etag = es_ids.get(r["uuid"])
        pg_etag = r['etag']
        if es_etag == pg_etag or (pg_etag == tombstone_etag and es_etag is None):
            continue

        if yield_record:
            yield r
        else:
            yield index_record(ei, rc, typ, r, do_index=False)


def queryIter(query, ei, rc, typ, yield_record=False):
    q = {
        "index": ei.indexName,
        "doc_type": typ,
        "_source": [],
        "size": 10000,
        "scroll": "10m"
    }

    for r in elasticsearch.helpers.scan(ei.es, query=query, **q):
        sql = "SELECT * FROM idigbio_uuids_data WHERE uuid=%s and type=%s"
        params = (r["_id"], typ[:-1])
        rec = apidbpool.fetchone(sql, params, cursor_factory=DictCursor)
        if rec is not None:
            if yield_record:
                yield rec
            else:
                yield index_record(ei, rc, typ, rec, do_index=False)


def uuidsIter(uuid_l, ei, rc, typ, yield_record=False, children=False):
    for rid in uuid_l:
        if children:
            logger.debug("Selecting children of %s.", rid)
            sql = "SELECT * FROM idigbio_uuids_data WHERE parent=%s and type=%s"
        else:
            sql = "SELECT * FROM idigbio_uuids_data WHERE uuid=%s and type=%s"
        params = (rid.strip(), typ[:-1])
        results = apidbpool.fetchall(sql, params, cursor_factory=DictCursor)
        for rec in results:
            if yield_record:
                yield rec
            else:
                yield index_record(ei, rc, typ, rec, do_index=False)


def delete(ei, rc, no_index=False):
    logger.info("Running deletes")

    count = 0
    sql = "SELECT id,type FROM uuids WHERE deleted=true"
    results = apidbpool.fetchiter(sql, named=True, cursor_factory=DictCursor)
    for r in results:
        count += 1
        if not no_index:
            ei.es.delete(**{
                "index": ei.indexName,
                "doc_type": r["type"] + 's',
                "id": r["id"]
            })

        if count % 10000 == 0:
            logger.info("%s", count)

    logger.info("%s", count)
    try:
        ei.optimize()
    except:
        pass


def resume(ei, rc, also_delete=False, no_index=False):
    # Create a partial application to add in the keyword argument
    f = functools.partial(type_yield_resume, also_delete=also_delete)
    consume(ei, rc, f, no_index=no_index)
    ei.optimize()

def full(ei, rc, no_index=False):
    logger.info("Begin 'full' indexing...")
    consume(ei, rc, type_yield, no_index=no_index)
    ei.optimize()

def incremental(ei, rc, no_index=False):
    consume(ei, rc, type_yield_modified, no_index=no_index)
    ei.optimize()

def query(ei, rc, query, no_index=False):
    f = functools.partial(queryIter, query)
    consume(ei, rc, f, no_index=no_index)
    ei.optimize()

def uuids(ei, rc, uuid_l, no_index=False, children=False):
    f = functools.partial(uuidsIter, uuid_l, children=children)
    consume(ei, rc, f, no_index=no_index)
    ei.optimize()

def consume(ei, rc, iter_func, no_index=False):
    for typ in ei.types:
        # Construct a version of index record that can be just called with the
        # record
        index_func = functools.partial(index_record, ei, rc, typ, do_index=False)

        to_index = iter_func(ei, rc, typ, yield_record=True)
        index_record_tuples = itertools.imap(index_func, to_index)

        if no_index:
            for _ in index_record_tuples:
                pass
        else:
            for ok, item in ei.bulk_index(index_record_tuples):
                # Is there a way to try/except the iterator to prevent Exceptions from being fatal?
                # Let's try!
                #
                # pass
                if not ok:
                    logger.warning('Failed during bulk index index: {0} '.format(item))
        # We should never need to call gc manually.  Can we drop this?  Especially
        # since we no longer ever use continuous mode.
        gc.collect() 

def continuous_incremental(ei, rc, no_index=False):
    while True:
        t_start = datetime.datetime.now()
        logger.info("Starting Incremental Run at %s", t_start.isoformat())
        incremental(ei, rc, no_index=no_index)
        t_end = datetime.datetime.now()
        logger.info("Ending Incremental Run from %s at %s",
                    t_start.isoformat(), t_end)
        sleep_duration = max(
            [MAX_SLEEP - (t_end - t_start).total_seconds(), MIN_SLEEP])
        logger.info("Sleeping for %s seconds", sleep_duration)
        time.sleep(sleep_duration)
