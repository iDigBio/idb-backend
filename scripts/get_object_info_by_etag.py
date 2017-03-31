# coding: utf-8
from __future__ import absolute_import

import argparse
import boto3
from idb import config


argparser = argparse.ArgumentParser(description='Script to check an s3 object by etag')
argparser.add_argument("-b", "--bucket", required=True, help="Name of s3 bucket. Example: idigbio-images-prod")
argparser.add_argument("-e", "--etag", required=True, help="etag of the object")
args = argparser.parse_args()

bucket = args.bucket
etag = args.etag

#bucket = 'idigbio-images-prod'
#etag = 'fc778eb00cae12784a5328e5ac1df2e1'

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

k.load()



def check_pub(k):
    return PUBLIC_READ in k.Acl().grants

print ("MIME type of " + etag + " is '" + k.content_type + "'")

print ("checking existence of PUBLIC acl...")
print (check_pub(k))

