"""This script is trying to fixup errors where images weren't properly marked public

This happened b/c sometimes there was an error in between uploading it
and making it public in the derivatives script.

While we're workin on it we were trying to update the mime type, but
the k.put request ended up erasing the objects!!!

"""
from __future__ import division, absolute_import, print_function

import gevent
from gevent import monkey, pool, queue
monkey.patch_all()

import time
import logging
from datetime import datetime
from collections import Counter

import enum
import boto3
import botocore.exceptions

from idb import config
from idb.helpers.memoize import memoized, filecached
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


Status = enum.Enum('Status', ['ok', 'fouled', 'missing', 'rederive', 'etagmismatch'])


def check_key(k, mime):
    try:
        if k.content_length == 0:
            return Status.fouled

        if mime and not check_mime(k, mime):
            logger.debug("Fixing %s, ACL+Mime", k)
            cs = "{0}/{1}".format(k.bucket_name, k.key)
            k.copy_from(CopySource=cs, ACL='public-read',
                        ContentType=mime, MetadataDirective="REPLACE")
            return Status.ok

        elif not check_pub(k):
            logger.debug("Fixing %s, ACL", k)
            k.Acl().put(ACL="public-read")
            return Status.ok


    except botocore.exceptions.ClientError as ce:
        if ce.response['Error']['Code'] == "404":
            logger.error("Missing %s alltogether!", k)
            return Status.missing
        else:
            raise

def keyfn(obj):
    bucket, etag, mime = obj
    bucket = 'idigbio-' + bucket + '-prod'
    conn = s3connection()

    k = conn.Object(bucket, etag)
    status = check_key(k, mime)
    if status is Status.fouled or status is Status.missing:
        return status
    elif k.e_tag != '"{0}"'.format(etag):
        logger.warning("%s/%s etag doesn't match ceph's etag: %s", bucket, etag, k.e_tag)
        return Status.etagmismatch

    for ext in ['webview', 'thumbnail', 'fullsize']:
        k = conn.Object(bucket + '-' + ext, etag + '.jpg')
        status = check_key(k, 'image/jpeg')
        if status is Status.fouled or status is Status.missing:
            apidbpool.execute("UPDATE objects SET derivatives=false WHERE etag = %s", (etag,))
            return Status.rederive
    return Status.ok


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
    time.sleep(3)
    start = datetime.now()
    results = Counter()
    count = 0
    try:
        for k, result in process_keys(keyfn, list(itemset)):
            count += 1
            if result is Status.ok or result is Status.rederive:
                itemset.remove(k)
            elif isinstance(result, Exception):
                raise result
            results[result] += 1
            if count % 100 == 0:
                rate = count / max([(datetime.now() - start).total_seconds(), 1])
                remaining = len(itemset) / rate
                logger.info("Processed %d records at %4.1f/s; %6.1fs remaining",
                            count, rate, remaining)
    finally:
        logger.info("Finished %d records, %r", count, results)



if __name__ == '__main__':
    configure_app_log(2, journal='auto')
    logging.getLogger('botocore').setLevel(20)
    logging.getLogger('boto3').setLevel(20)
    logging.getLogger('boto3.resources.action').setLevel(24)
    kickstart()
