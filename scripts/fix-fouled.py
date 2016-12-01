"""This script is trying to find(and fix some of) errors made by ./reset-public.py

That script accidently overwrote object contents while trying to
adjust metadata. we know the set of objects it was working with, so go
through those and see which ones are missing; if we can restore it
from the fullsize bucket then do so.

"""
from __future__ import division, absolute_import, print_function

import gevent
from gevent import monkey, pool, queue
monkey.patch_all()

import time
import cPickle
import json
import atexit
import logging
from datetime import datetime
from collections import Counter
from functools import wraps

import boto3
import botocore.exceptions

from idb import config
from idb.helpers.memoize import memoized, filecached
from idb.helpers.storage import IDigBioStorage
from idb.helpers.logging import getLogger, configure_app_log
from idb.postgres_backend import apidbpool, cursor
from idigbio_ingestion.mediaing.fetcher import FetchItem

logger = getLogger("cephwalker")
store = IDigBioStorage()


@memoized()
def s3connection():
    return boto3.resource(
        's3',
        aws_access_key_id=config.IDB_STORAGE_ACCESS_KEY,
        aws_secret_access_key=config.IDB_STORAGE_SECRET_KEY,
        endpoint_url="https://s.idigbio.org")


def process_keys(fn, keylist, poolsize=20):

    def wkfn(k):
        retries = 3
        attempt = 1
        while True:
            try:
                return (k, fn(k))
            except botocore.exceptions.ClientError as ce:
                logger.exception("Failed operation on storage, attempt %s/%s", attempt, retries)
                attempt += 1
                if attempt > retries:
                    return (k, ce)
                time.sleep(2 ** (attempt + 1))
            except Exception as e:
                logger.exception("Unexpected exception handling %s", k)
                return (k, e)

    p = pool.Pool(poolsize)
    return p.imap_unordered(wkfn, keylist)

def geturls(etag):
    sql = """SELECT DISTINCT url
             FROM media_objects
             WHERE etag LIKE %s"""
    return set(u[0] for u in apidbpool.fetchall(sql, (etag,), cursor_factory=cursor))

def keyfn(obj):
    bucket, etag, mime = obj

    sql = """SELECT DISTINCT url
             FROM media_objects
             WHERE etag LIKE %s"""
    urls = set(u[0] for u in apidbpool.fetchall(sql, (etag,), cursor_factory=cursor))
    for url in urls:
        fi = FetchItem(url, bucket, mime)
        if not fi.prefix:
            continue
        fi.get_media()
        fi.upload_to_storage(store)
        fi.cleanup()
        if not fi.ok:
            continue
        else:
            apidbpool.execute("UPDATE objects SET derivatives=false WHERE etag LIKE %s", (etag,))
            logger.info("Downloaded %s from %s", etag, url)
            return "downloaded"
    else:
        return "novalidurl"


@filecached("/tmp/fouled.picklecache")
def getfouled():
    filename = '/root/definitely-fouled.json'
    logger.debug("Reading from %s", filename)
    with open(filename, 'rb') as f:
        fouled = set(tuple(e) for e in json.load(f))
    return fouled


def kickstart():
    fouled = getfouled()
    logger.info("Found %s records to check", len(fouled))
    time.sleep(3)
    start = datetime.now()
    results = Counter()
    count = 0
    for (k, result) in process_keys(keyfn, list(fouled)):
        count += 1
        results[result] += 1
        if result == "downloaded":
            fouled.remove(k)
        elif isinstance(result, Exception):
            results['erred'] += 1
        if count % 100 == 0:
            rate = count / max([(datetime.now() - start).total_seconds(), 1])
            remaining = len(fouled) / rate
            logger.info("Checked %d records at %4.1f/s; %6.1fs remaining; downloaded:%s, nomime:%s, novalidurl:%s",
                        count, rate, remaining, results["downloaded"], results["nomime"], results["novalidurl"])

    logger.info("Checked %d records;  %r", count, results)


if __name__ == '__main__':
    configure_app_log(2, journal='auto')
    logging.getLogger('boto3.resources.action').setLevel(25)
    logging.getLogger('botocore').setLevel(20)
    logging.getLogger('boto3').setLevel(20)
    kickstart()
