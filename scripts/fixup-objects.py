"""This script goes through objects in the DB and fixes any ceph errors we can

This originally started because images weren't properly marked public
and didn't always have the correct mime type set in metadata. However
the original version of the script overwrote some objects in ceph with
0 byte versions. This necessitated going back through all the keys and
trying deeper levels of repair. This includes:

 * Is the version in the -fullsize bucket a suitable replacement?
 * Can we redownload from the original source?
 * Do we need to redo derivates


systemd-run --unit=fixup-objects  \
    --description "Checking metadata and health of objects" \
    --no-block \
    --property KillSignal=SIGINT \
    python /root/idb-backend/scripts/fixup-objects.py
"""
from __future__ import division, absolute_import, print_function

import gevent
from gevent import monkey, pool, queue
monkey.patch_all()

import time
import logging
from datetime import datetime
from collections import Counter
from functools import wraps

import enum
import boto3
import botocore.exceptions

from idb import config
from idb.helpers.memoize import memoized, filecached
from idb.helpers.logging import getLogger, configure_app_log
from idb.postgres_backend import apidbpool, cursor
from idigbio_ingestion.mediaing.fetcher import FetchItem


logger = getLogger("cephwalker")

Status = enum.Enum('Status', ['ok', 'fouled', 'missing', 'rederive', 'etagmismatch'])


@filecached('/tmp/checkitems.picklecache')
def getitems():
    sql = """SELECT objects.bucket, objects.etag, objects.detected_mime as mime
             FROM objects
             JOIN media_objects USING (etag)
             WHERE media_objects.modified > '2016-08-01'
               AND derivatives = true
    """
    return set(apidbpool.fetchall(sql, cursor_factory=cursor))


def kickstart(itemset=None, poolsize=20):
    if itemset is None:
        itemset = getitems()
        logger.info("Found %s records to check", len(itemset))
        time.sleep(3)
    start = datetime.now()
    results = Counter()
    count = 0
    p = pool.Pool(poolsize)

    def wkfn(k):
        try:
            return (k, objfn(k))
        except Exception as e:
            logger.exception("Failed on %r", k)
            return (k, e)

    try:
        for k, result in p.imap_unordered(wkfn, list(itemset)):
            count += 1
            if isinstance(result, Exception):
                raise result
            if result is Status.ok or result is Status.rederive:
                itemset.remove(k)
            results[result] += 1
            if count % 100 == 0:
                rate = count / max([(datetime.now() - start).total_seconds(), 1])
                remaining = len(itemset) / rate
                logger.info("Processed %d records at %4.1f/s; %6.1fs remaining",
                            count, rate, remaining)
    finally:
        logger.info("Finished %d records, %r", count, results)


def boto3retried(retries=3):
    "Decorator to watch for boto3 errors and do a backoff retry loop."
    def getfn(fn):
        @wraps(fn)
        def thunk(*args, **kwargs):
            attempt = 1
            while True:
                try:
                    return fn(*args, **kwargs)
                except botocore.exceptions.ClientError as ce:
                    if ce.response['Error']['Code'] and 400 <= int(ce.response['Error']['Code']) < 500:
                        raise
                    logger.exception("Failed operation on storage, attempt %s/%s", attempt, retries)
                    attempt += 1
                    if attempt > retries:
                        raise
                    time.sleep(2 ** (attempt + 1))
        return thunk
    return getfn


@boto3retried()
def objfn(obj):
    """Entry point for checking an object from the database

    This attemps to do all fixups/repairs we can on all
    buckets/versions of the given object from the database.
    """
    bucket, etag, mime = obj
    k = getkey(obj)
    status = check_key(k, mime=mime, etag=etag)

    if status is not Status.ok:
        if copy_from_fullsize(obj) is Status.ok:
            # original obj is now ok, continue to check derivatives though
            status = Status.ok

    if status is not Status.ok:
        return redownload(obj)

    for ext in ['webview', 'thumbnail', 'fullsize']:
        k = getkey(obj, ext)
        status = check_key(k, mime='image/jpeg')
        if status is not Status.ok:
            return forcerederive(obj)
    return Status.ok


PUBLIC_READ = {u'Grantee': {u'Type': 'Group',
                            u'URI': 'http://acs.amazonaws.com/groups/global/AllUsers'},
               u'Permission': 'READ'}


def check_key(k, mime=None, etag=None):
    """Examine a specific ceph key for proper permissions, etag, mime

    If it can be fixed in place by updating metadata then do so
    """
    try:
        if k.content_length == 0:
            return Status.fouled

        if etag and k.e_tag != '"{0}"'.format(etag):
            logger.warning("%s/%s etag doesn't match ceph's etag: %s", k.bucket_name, etag, k.e_tag)
            return Status.etagmismatch

        if mime and k.content_type != mime:
            logger.debug("%s/%s Fixing ACL+Mime", k.bucket_name, k.key)
            cs = "{0}/{1}".format(k.bucket_name, k.key)
            k.copy_from(CopySource=cs, ACL='public-read',
                        ContentType=mime, MetadataDirective="REPLACE")

        elif PUBLIC_READ not in k.Acl().grants:
            logger.debug("%s/%s Fixing ACL", k.bucket_name, k.key)
            k.Acl().put(ACL="public-read")

        return Status.ok
    except botocore.exceptions.ClientError as ce:
        if ce.response['Error']['Code'] == "404":
            logger.error("%s/%s is missing!", k.bucket_name, k.key)
            return Status.missing
        else:
            raise

def getkey(obj, deriv=None):
    bucket, etag, mime = obj
    bucket = 'idigbio-{0}-{1}'.format(bucket, config.ENV)
    conn = s3connection()
    if deriv:
        bucket = '{0}-{1}'.format(bucket, deriv)
        name = etag + '.jpg'
    else:
        name = etag
    return conn.Object(bucket, name)


def copy_from_fullsize(obj):
    """Try restoring an object from the -fullsize bucket

    This will only work if the image orignally was a jpeg. We detect
    this by seeing if the etag matches precisely.

    """
    bucket, etag, mime = obj
    k = getkey(obj)
    fk = getkey(obj, 'fullsize')
    try:
        fk.load()
    except botocore.exceptions.ClientError as ce:
        if ce.response['Error']['Code'] and 400 <= int(ce.response['Error']['Code']) < 500:
            return Status.missing
        raise
    if fk.e_tag != '"{0}"'.format(etag):
        return Status.etagmismatch

    src = fk.bucket_name + '/' + fk.key
    k.copy_from(ACL='public-read', ContentType='image/jpeg', CopySource=src)
    logger.info("%s/%s from fullsize", k.bucket_name, etag)
    return Status.ok


def geturls(etag):
    sql = """SELECT DISTINCT url
             FROM media_objects
             WHERE etag LIKE %s"""
    return set(u[0] for u in apidbpool.fetchall(sql, (etag,), cursor_factory=cursor))


def redownload(obj):
    """Go through any urls associated with the etag and try to fetch them

    In doing so we only accept objects with matching etag. I.e. a
    precise replacement

    """
    bucket, etag, mime = obj
    for url in geturls(etag):
        fi = FetchItem(url, bucket, mime)
        try:
            if not fi.prefix:
                continue
            fi.get_media()
            mo = fi.media_object
            if not fi.ok:
                continue
            elif mo.etag != etag:
                logger.info(
                    "%s/%s downloaded different etag from %s",
                    mo.bucketname, etag, url)
                continue
            targetkey = getkey(obj)
            targetkey.put(Body=fi.content, ACL='public-read', ContentType=mo.detected_mime)
            logger.info("%s/%s Downloaded from %s", targetkey.bucket_name, etag, url)
            return forcerederive(obj)
        finally:
            fi.cleanup()
    else:
        return Status.missing


def forcerederive(obj):
    "Mark an object in the database as needing rederive"
    bucket, etag, mime = obj
    logger.debug("idigbio-%s-%s/%s forcing rederive", bucket, config.ENV, etag)
    apidbpool.execute("UPDATE objects SET derivatives=false WHERE etag=%s", (etag,))
    return Status.rederive


@memoized()
def s3connection():
    return boto3.resource(
        's3',
        aws_access_key_id=config.IDB_STORAGE_ACCESS_KEY,
        aws_secret_access_key=config.IDB_STORAGE_SECRET_KEY,
        endpoint_url="https://s.idigbio.org")


if __name__ == '__main__':
    configure_app_log(2, journal='auto')
    logging.getLogger('botocore').setLevel(20)
    logging.getLogger('boto3').setLevel(20)
    logging.getLogger('boto3.resources.action').setLevel(24)
    kickstart()
