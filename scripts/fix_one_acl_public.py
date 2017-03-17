# coding: utf-8
from __future__ import absolute_import

import boto3
from idb import config

bucket = 'idigbio-images-prod'
etag = 'fc778eb00cae12784a5328e5ac1df2e1'

PUBLIC_READ = {u'Grantee': {u'Type': 'Group',
                            u'URI': 'http://acs.amazonaws.com/groups/global/AllUsers'},
               u'Permission': 'READ'}

def s3connection():
    return boto3.resource(
        's3',
        aws_access_key_id=config.IDB_STORAGE_ACCESS_KEY,
        aws_secret_access_key=config.IDB_STORAGE_SECRET_KEY,
        endpoint_url="https://s.idigbio.org")

conn = s3connection()

k = conn.Object(bucket, etag)

result = k.Acl().put(ACL="public-read")

print(result)
