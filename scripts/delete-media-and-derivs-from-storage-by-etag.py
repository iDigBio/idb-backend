# coding: utf-8
from __future__ import absolute_import

import argparse
import boto3
from idb import config

big_help_text = """
    Script to delete s3 object and derivatives based on media appliance reference etag.
    Currently only works on images in the prod buckets. Does not purge references
    from database.

    For example, input supplied is an etag:

      011bc453bc47b663eb3659aef9ce8358

    The script will delete the s3 objects from buckets named:

      idigbio-images-prod
      idigbio-images-prod-thumbnail
      idigbio-images-prod-webview
      idigbio-images-prod-fullsize     
"""

argparser = argparse.ArgumentParser(description=big_help_text, formatter_class=argparse.RawDescriptionHelpFormatter)
#argparser.add_argument("-b", "--bucket", required=True, help="Name of s3 bucket. Example: idigbio-images-prod")
#argparser.add_argument("-t", "--type", required=True, help="Type of media object, used to construct bucket names. Example: images")
arg_group = argparser.add_mutually_exclusive_group(required=True)
arg_group.add_argument("-e", "--etag", help="etag of a single object")
arg_group.add_argument("-i", "--infile", help="filepath containing list of etags to delete, one per line") 
args = argparser.parse_args()

if args.etag:
    etags = [args.etag]
else:
    etags = []
    with open (args.infile) as inputfile:
        for line in inputfile:
            etags.append(line.strip())
    
#bucketlist = ['idigbio-images-prod', 'idigbio-images-prod-thumbnail', 'idigbio-images-prod-webview', 'idigbio-images-prod-fullsize']
deriv_buckets = ['idigbio-images-prod-thumbnail', 'idigbio-images-prod-webview', 'idigbio-images-prod-fullsize']

api_media_base_url = 'https://api.idigbio.org/v2/media/'

def s3connection():
    return boto3.resource(
        's3',
        aws_access_key_id=config.IDB_STORAGE_ACCESS_KEY,
        aws_secret_access_key=config.IDB_STORAGE_SECRET_KEY,
        endpoint_url="https://s.idigbio.org")

def delete_by_bucket_and_etag (conn, bucket, s3_etag):
    try:
        k = conn.Object(bucket, s3_etag)
        e_tag = k.e_tag
        k.delete()
        print ('Found for Delete,{0},{1},{2}'.format(bucket, s3_etag, e_tag))
    except (KeyboardInterrupt, SystemExit):
        raise
    except:
        print ('Not Found,{0},{1}'.format(bucket, s3_etag))


s3conn = s3connection()

for e in etags:
    delete_by_bucket_and_etag (s3conn, 'idigbio-images-prod', e)
    for b in deriv_buckets:
        delete_by_bucket_and_etag (s3conn, b, e + '.jpg')

