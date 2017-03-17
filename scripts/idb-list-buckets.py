# coding: utf-8
from __future__ import absolute_import

import boto3
from idb import config

def s3connection():
    return boto3.resource(
        's3',
        aws_access_key_id=config.IDB_STORAGE_ACCESS_KEY,
        aws_secret_access_key=config.IDB_STORAGE_SECRET_KEY,
        endpoint_url="https://s.idigbio.org")

conn = s3connection()

for bucket in conn.buckets.all():
    print(bucket.name)
