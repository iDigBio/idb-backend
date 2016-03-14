"""
    Thin wrapper around boto.s3 to ensure consistent access patterns in idigbio scripts
"""
from __future__ import absolute_import
import os
import sys
import glob
import subprocess

import boto
import boto.s3.connection
from boto.s3.key import Key

from idb.helpers.media_validation import get_validator
from idb.postgres_backend.db import PostgresDB
from idb.helpers.etags import calcFileHash
from idb.config import config

class IDigBioStorage(object):
    """
        Class to abstract out the iDigBio S3 storage.

        Note:
            You must either set access_key and secret_key when
            initializing the object, or (prefered) set the
            IDB_STORAGE_ACCESS_KEY and IDB_STORAGE_SECRET_KEY
            environemtn variables.
    """

    def __init__(self,host="s.idigbio.org",access_key=None,secret_key=None):
        self.db = PostgresDB()

        if access_key is None:
            access_key = os.getenv("IDB_STORAGE_ACCESS_KEY")

        if secret_key is None:
            secret_key = os.getenv("IDB_STORAGE_SECRET_KEY")

        self.host = host

        assert access_key is not None
        assert secret_key is not None

        self.boto_conn = boto.connect_s3(
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
            host=host,
            is_secure=False,
            calling_format=boto.s3.connection.OrdinaryCallingFormat(),
        )

    def get_bucket(self,bucket_name):
        """
            Return a boto.s3.Bucket object for the requested bucket.
        """
        return self.boto_conn.get_bucket(bucket_name,validate=False)

    def get_key(self,key_name,bucket_name,bucket=None):
        """
            Return a boto.s3.Key object for the requested bucket name and key name.
            If you have already instantiated a bucket object, you may pass that and
            the method will use the existing object instead of creating a new one.
        """
        if bucket is not None:
            return bucket.get_key(key_name,validate=False)
        else:
            return self.get_bucket(bucket_name).get_key(key_name,validate=False)

    def get_link(self,key_name,bucket_name,secure=False):
        if secure:
            return "https://{0}/{1}/{2}".format(self.host,bucket_name,key_name)
        else:
            return "http://{0}/{1}/{2}".format(self.host,bucket_name,key_name)

    def split_file(self,s3_key_name, in_file, mb_size, split_num=5):
        prefix = os.path.join(os.path.dirname(in_file), "%sS3PART" % (os.path.basename(s3_key_name)))
        split_size = int(min(mb_size / (split_num * 2.0), 250))
        if not os.path.exists("%saa" % prefix):
            cl = ["split", "-b%sm" % split_size, in_file, prefix]
            subprocess.check_call(cl)
        return sorted(glob.glob("%s*" % prefix))

    def upload_file(self,key_name,bucket_name,file_name):
        bucket = self.get_bucket(bucket_name)
        k = self.get_key(key_name,bucket_name)
        mb_size = os.path.getsize(file_name) / 1e6
        if mb_size > 1000:
            parts = self.split_file(key_name,file_name,mb_size)
            mp = bucket.initiate_multipart_upload(key_name)
            try:
                for i,part in enumerate(parts):
                    with open(part,"rb") as pf:
                        mp.upload_part_from_file(pf,i+1)
                    os.unlink(part)
                mp.complete_upload()
            except Exception, e:
                mp.cancel_upload()
                raise e
        else:
            k.set_contents_from_filename(file_name)
        k.make_public()
        return k

    def get_file_by_url(self,url, file_name=None):
        cur = self.db.cursor()

        cur.execute("""SELECT objects.bucket, objects.etag
            FROM media
            LEFT JOIN media_objects ON media.url = media_objects.url
            LEFT JOIN objects on media_objects.etag = objects.etag
            WHERE media.url=%(url)s
        """, {"url": url})

        r = cur.fetchone()

        k = self.get_key(r["etag"],"idigbio-{}-prod".format(r["bucket"]))

        if file_name is None:
            file_name = r["etag"]

        k.get_contents_to_filename(file_name)
        return file_name

    # def set_file_by_url(self, url, fil, typ, mime=None):
    #     fname = fil.filename
    #     h, size = calcFileHash(fil,op=False,return_size=True)
    #     fil.seek(0)

    #     validator = get_validator(mime)
    #     valid, detected_mime = validator(fname,typ,mime,fil.read(1024))
    #     fil.seek(0)

    #     self.upload_file(h,"idigbio-{}-prod".format(typ),fname)

    #     current_app.config["DB"]._cur.execute("INSERT INTO media (url,type,mime,last_status,last_check,owner) VALUES (SELECT %s,%s,%s,200,now(),%s WHERE NOT EXISTS (SELECT 1 FROM media WHERE url=%s))", (url,typ,detected_mime, config["env"]["IDB_UUID"],url))
    #     current_app.config["DB"]._cur.execute("INSERT INTO objects (bucket, etag, detected_mime) (SELECT %s, %s, %s WHERE NOT EXISTS (SELECT 1 FROM objects WHERE etag=%s))", (typ, h, detected_mime, h))
    #     current_app.config["DB"]._cur.execute("INSERT INTO media_objects (url, etag) VALUES (%s,%s)", (url,h))
