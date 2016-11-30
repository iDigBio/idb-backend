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
from idb.helpers.memoize import memoized
from idb.helpers.logging import getLogger, configure_app_log
from idb.postgres_backend import apidbpool, cursor


logger = getLogger("cephwalker")

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
            except (DestroyedObjectError, MissingOriginalError) as doe:
                return (k, doe)
            except Exception as e:
                logger.exception("Unexpected exception handling %s", k)
                return (k, e)

    p = pool.Pool(poolsize)
    return p.imap_unordered(wkfn, keylist)


PUBLIC_READ = {u'Grantee': {u'Type': 'Group',
                            u'URI': 'http://acs.amazonaws.com/groups/global/AllUsers'},
               u'Permission': 'READ'}

def check_mime(k, mime):
    return k.content_type == mime

def check_pub(k):
    return PUBLIC_READ in k.Acl().grants


class MissingOriginalError(Exception):
    pass

class DestroyedObjectError(Exception):
    pass

def keyfn(obj):
    bucket, etag, mime = obj
    bucket = 'idigbio-' + bucket + '-prod'
    conn = s3connection()

    k = conn.Object(bucket, etag)
    try:
        k.load()
    except botocore.exceptions.ClientError as ce:
        if ce.response['Error']['Code'] == "404":
            raise MissingOriginalError()
    if k.content_length == 0:
        fullk = conn.Object(bucket + '-fullsize', etag + '.jpg')
        try:
            fullk.load()
        except botocore.exceptions.ClientError as ce:
            return "gone"
        if fullk.content_length == 0:
            return "gone"
        if mime == 'image/jpeg' or fullk.e_tag == '"{0}"'.format(etag):
            src = fullk.bucket_name + '/' + fullk.key
            k.copy_from(ACL='public-read', ContentType='image/jpeg', CopySource=src)
            logger.debug("Restored %s from fullsize", etag)
        else:
            return "gone"

    for ext in ['webview', 'thumbnail', 'fullsize']:
        k = conn.Object(bucket + '-' + ext, etag + '.jpg')
        if k.content_length == 0:
            apidbpool.execute("UPDATE objects SET derivatives=false WHERE etag LIKE %s", (etag,))
            return "rederive"
    return "fine"

def filecached(filename):
    def writecache(value):
        logger.info("Writing back cache file to %s", filename)
        with open(filename, 'wb') as f:
            cPickle.dump(value, f)

    def getfn(fn):
        @wraps(fn)
        def thunk():
            val = None
            try:
                with open(filename, 'rb') as f:
                    logger.info("Reading cache file from %s", filename)
                    val = cPickle.load(f)
            except (IOError, EOFError):
                val = fn()
                writecache(val)
            atexit.register(writecache, val)
            return val
        return thunk
    return getfn


def allitems():
    sql = """SELECT objects.bucket, objects.etag, objects.detected_mime as mime
             FROM objects
             JOIN media_objects USING (etag)
             WHERE media_objects.modified > '2016-08-01'
               AND derivatives = true
    """
    return set(apidbpool.fetchall(sql, cursor_factory=cursor))

def untoucheditems():
    "These are the items we never touched in the original bad script"
    with open("/home/nbird/projects/idigbio/untouched.picklecache", 'rb') as f:
        return cPickle.load(f)

@filecached('/tmp/possiblyfouled.picklecache')
def possiblyfouled():
    return allitems() - untoucheditems()

# @filecached('/tmp/checkitems.picklecache')
# def getitems():
#     sql = """SELECT objects.bucket, objects.etag, objects.detected_mime as mime
#              FROM objects
#              JOIN media_objects USING (etag)
#              WHERE media_objects.modified > '2016-08-01'
#                AND derivatives = true
#     """
#     return set(apidbpool.fetchall(sql, cursor_factory=cursor))

def kickstart():
    itemset = possiblyfouled()
    logger.info("Found %s records to check", len(itemset))
    time.sleep(3)
    start = datetime.now()
    results = Counter()
    count, fine, rederive = 0, 0
    for (k, result) in process_keys(keyfn, list(itemset)):
        count += 1
        results[result] += 1
        if result == "fine" or result == "rederive":
            itemset.remove(k)
        if count % 100 == 0:
            rate = count / max([(datetime.now() - start).total_seconds(), 1])
            remaining = len(itemset) / rate
            logger.info("Checked %d records at %4.1f/s; %6.1fs remaining; gone:%s, rederive:%s",
                        count, rate, remaining, results["gone"], results["rederive"])

    logger.info("Checked %d records;  %r", count, results)

    with open('/home/nbird/projects/idigbio/definitely-fouled.json', 'wb') as f:
        json.dump(list(itemset), f)


if __name__ == '__main__':
    configure_app_log(2)
    logging.getLogger('boto3.resources.action').setLevel(25)
    logging.getLogger('botocore').setLevel(20)
    logging.getLogger('boto3').setLevel(20)
    kickstart()
