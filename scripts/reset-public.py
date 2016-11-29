from __future__ import division, absolute_import, print_function

import gevent
from gevent import monkey, pool, queue
monkey.patch_all()

from datetime import datetime
import time
import cPickle
import atexit
import logging

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
    workqueue = queue.Queue(items=keylist)
    successful, failed = queue.Queue(), queue.Queue()

    def wkfn(k):
        retries = 3
        attempt = 1
        while True:
            try:
                successful.put((k, fn(k)))
                return
            except botocore.exceptions.ClientError as ce:
                logger.exception("Failed operation on storage, attempt %s/%s", attempt, retries)
                attempt += 1
                if attempt > retries:
                    failed.put((k, ce))
                time.sleep(2 ** (attempt + 1))
            except Exception as e:
                logger.exception("Unexpected exception handling %s", k)
                failed.put((k, e))

    def worker():
        while not workqueue.empty():
            wkfn(workqueue.get())

    p = pool.Pool(poolsize)
    while not p.full():
        p.spawn(worker)

    def waiter():
        p.join()
        successful.put(StopIteration)
        failed.put(StopIteration)
    gevent.spawn(waiter)
    return successful, failed


PUBLIC_READ = {u'Grantee': {u'Type': 'Group',
                            u'URI': 'http://acs.amazonaws.com/groups/global/AllUsers'},
               u'Permission': 'READ'}

def check_mime(k, mime):
    return k.content_type == mime

def check_pub(k):
    return PUBLIC_READ in k.Acl().grants


class MissingOriginalError(Exception):
    pass

def keyfn(obj):
    bucket, etag, mime = obj
    bucket = 'idigbio-' + bucket + '-prod'
    conn = s3connection()

    k = conn.Object(bucket, etag)
    try:
        if mime and not check_mime(k, mime):
            k.put(ACL="public-read", ContentType=mime)
        elif not check_pub(k):
            k.put(ACL="public-read")

    except botocore.exceptions.ClientError as ce:
        if ce.response['Error']['Code'] == "404":
            logger.error("Missing %s alltogether!", k)
            raise MissingOriginalError()
        else:
            raise

    for ext in ['webview', 'thumbnail', 'fullsize']:
        k = conn.Object(bucket + '-' + ext, etag + '.jpg')
        try:
            mime = 'image/jpeg'
            if not (check_mime(k, mime) and check_pub(k)):
                k.put(ACL="public-read", ContentType=mime)
        except botocore.exceptions.ClientError as ce:
            if ce.response['Error']['Code'] == "404":
                logger.warn("NoDeriv %s", k)
                apidbpool.execute(
                    "UPDATE objects SET derivatives=false WHERE etag LIKE %s", (etag,))
                return True  # derivatives will redo this entire etag and fix it up.
            else:
                raise
    return True


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


@filecached('/tmp/checkitems.picklecache')
def getitems():
    sql = """SELECT objects.bucket, objects.etag, objects.detected_mime as mime
             FROM objects
             JOIN media_objects USING (etag)
             WHERE media_objects.modified > '2016-08-01'
               AND derivatives = true
    """
    return set(apidbpool.fetchall(sql, cursor_factory=cursor))

def kickstart():
    itemset = getitems()
    logger.info("Found %s records to check", len(itemset))
    start = datetime.now()
    successful, failed = process_keys(keyfn, itemset)
    count = 0
    for (k, result) in successful:
        count += 1
        itemset.remove(k)
        if count % 100 == 0:
            rate = count / max([(datetime.now() - start).total_seconds(), 1])
            remaining = len(itemset) / rate
            logger.info("Processed %d records at %4.1f/s; %6.1fs remaining",
                        count, rate, remaining)

    for (k, err) in failed:
        logger.info("%r failed with %r", k, err)


if __name__ == '__main__':
    configure_app_log(2)
    logging.getLogger('boto3.resources.action').setLevel(25)
    kickstart()
