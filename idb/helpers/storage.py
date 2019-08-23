"""
    Thin wrapper around boto.s3 to ensure consistent access patterns in idigbio scripts
"""
from __future__ import division, absolute_import, print_function

import cStringIO
import math
import os
import time

import boto
import boto.s3.connection
from boto.exception import BotoServerError, BotoClientError, S3DataError

from idb import config
from idb.helpers.logging import idblogger
from idb.postgres_backend.db import MediaObject

logger = idblogger.getChild('storage')

private_buckets = {
    "debugfile"
}


class IDigBioStorage(object):
    """
        Class to abstract out the iDigBio S3 storage.

        Note:
            You must either set access_key and secret_key when
            initializing the object, or (prefered) set the
            IDB_STORAGE_ACCESS_KEY and IDB_STORAGE_SECRET_KEY
            environment variables.
    """

    def __init__(self, host=None, access_key=None, secret_key=None):

        self.host = host = host or config.IDB_STORAGE_HOST
        access_key = access_key or config.IDB_STORAGE_ACCESS_KEY
        secret_key = secret_key or config.IDB_STORAGE_SECRET_KEY
        assert self.host, "Must specify s3 host"
        assert access_key, "Must specify s3 access_key"
        assert secret_key, "Must specify s3 secret_key"

        port = None
        if ':' in host:
            host, port = host.split(':')
            port = int(port)
        self.boto_conn = boto.connect_s3(
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
            host=host,
            port=port,
            is_secure=False,
            calling_format=boto.s3.connection.OrdinaryCallingFormat(),
        )
        logger.debug("Initialized IDigBioStorage connection (boto.connect_s3) to {0}:{1}".format(host,port))

    def get_bucket(self, bucket_name):
        """Return a boto.s3.Bucket object for the requested bucket.
        """
        return self.boto_conn.get_bucket(bucket_name, validate=False)

    def get_key(self, key_name, bucket_name, bucket=None):
        """
            Return a boto.s3.Key object for the requested bucket name and key name.
            If you have already instantiated a bucket object, you may pass that and
            the method will use the existing object instead of creating a new one.
        """
        if bucket is not None:
            return bucket.get_key(key_name, validate=False)
        else:
            return self.get_bucket(bucket_name).get_key(key_name, validate=False)

    # def delete_key(...)
        # No separate delete function is needed since the key object itself has a delete() method.
        # e.g.
        #       k = get_key ('KEY_NAME', 'BUCKET_NAME')
        #       k.delete()  # works on the boto.s3.Key object

    def get_link(self, key_name, bucket_name, secure=False):
        if secure:
            return "https://{0}/{1}/{2}".format(self.host, bucket_name, key_name)
        else:
            return "http://{0}/{1}/{2}".format(self.host, bucket_name, key_name)

    MAX_CHUNK_SIZE = 1024 ** 3  # 1GiB

    def _get_size(self, fobj):
        loc = fobj.tell()
        fobj.seek(0, os.SEEK_END)
        size = fobj.tell()
        fobj.seek(loc, os.SEEK_SET)
        return size

    def upload(self, key, fobj, content_type=None, public=None, md5=None, multipart='auto', size=None):
        if public is None:
            public = key.bucket not in private_buckets

        if hasattr(fobj, 'fileno') or hasattr(fobj, 'read'):
            pass
        elif hasattr(fobj, 'open'):
            fobj = fobj.open('rb')
        elif isinstance(fobj, basestring):
            fobj = open(fobj, 'rb')
        elif not all(hasattr(fobj, a) for a in ('seek', 'tell', 'read')):
            raise ValueError("Unknown fobj type:", fobj)
        if size is None and multipart == 'auto':
            size = self._get_size(fobj)
        if content_type:
            key.set_metadata('Content-Type', content_type)

        if multipart == 'auto' and size > self.MAX_CHUNK_SIZE:
            self._upload_multipart(key, fobj, size)
        else:
            if md5:
                md5 = key.get_md5_from_hexdigest(md5)
            self.retry_loop(lambda: key.set_contents_from_file(fobj, rewind=True, md5=md5))
        if public:
            self.retry_loop(key.make_public)
        return key

    def _upload_multipart(self, k, fobj, size):
        chunk_count = int(math.ceil(size / self.MAX_CHUNK_SIZE))
        logger.debug("Starting upload to %r in %d chunks", k, chunk_count)
        try:
            mp = k.bucket.initiate_multipart_upload(k.name)

            def onepart(i):
                logger.debug("Uploading part %d", i)
                offset = i * self.MAX_CHUNK_SIZE
                usize = min([self.MAX_CHUNK_SIZE, size - offset])
                fobj.seek(offset)
                mp.upload_part_from_file(fp=fobj, part_num=i + 1, size=usize)
                logger.debug("Done part %d", i)

            for i in range(chunk_count):
                self.retry_loop(lambda: onepart(i))
            mp.complete_upload()
        except Exception:
            mp.cancel_upload()
            raise

    @staticmethod
    def retry_loop(attemptfn, retries=3):
        attempt = 1
        while True:
            try:
                return attemptfn()
            except (BotoServerError, BotoClientError):
                logger.exception("Failed operation on storage, attempt %s/%s", attempt, retries)
                attempt += 1
                if attempt > retries:
                    raise
                time.sleep(2 ** (attempt + 1))

    def get_key_by_url(self, url, idbmodel=None):
        mo = MediaObject.fromurl(url, idbmodel)
        if mo is None:
            raise Exception("No media with url {0!r}".format(url))
        return self.get_key(mo.keyname, mo.bucketname)

    def get_file_by_url(self, url, file_name=None):
        k = self.get_key_by_url(url)
        if file_name is None:
            file_name = k.name
        IDigBioStorage.get_contents_to_filename(k, file_name)
        return file_name

    def get_key_by_etag(self, etag, idbmodel=None):
        mo = MediaObject.frometag(etag, idbmodel=idbmodel)
        return self.get_key(mo.keyname, mo.bucketname)

    def fetch(self, key_name, bucket_name, filename, md5=None):
        k = self.get_key(key_name, bucket_name)
        return self.get_contents_to_filename(k, filename, md5=md5)

    @staticmethod
    def get_contents_to_filename(key, filename, md5=None):
        """Wraps ``key.get_contents_to_filename`` ensuring an atomic fetch

        The default version on a key will leave partial results if the

        :param boto.s3.key.Key key: The key to fetch
        :param str filename: The filename to fetch into
        :param str md5: If given compare the downloaded md5 to the given digest hash

        """
        from atomicfile import AtomicFile
        with AtomicFile(filename, mode='wb') as af:
            IDigBioStorage.get_contents_to_file(key, af, md5=md5)
        return filename

    @staticmethod
    def get_contents_to_mem(key, md5=None):
        "Wraps ``key.get_contents_to_file fetching into a StringIO buffer``"
        buff = cStringIO.StringIO()
        IDigBioStorage.get_contents_to_file(key, buff, md5=md5)
        buff.seek(0)
        return buff

    @staticmethod
    def get_contents_to_file(key, fp, md5=None):
        key.get_contents_to_file(fp)
        if md5 and key.md5 != md5:
            raise S3DataError(
                'MD5 of downloaded did not match given MD5: '
                '%s vs. %s' % (key.md5, md5))
        return key


    # def set_file_by_url(self, url, fil, typ, mime=None):
    #     fname = fil.filename
    #     h, size = calcFileHash(fil,op=False,return_size=True)
    #     fil.seek(0)

    #     validator = get_validator(mime)
    #     valid, detected_mime = validator(fname,typ,mime,fil.read(1024))
    #     fil.seek(0)

    #     self.upload_file(h,"idigbio-{}-prod".format(typ),fname)

    #     current_app.config["DB"]._cur.execute("INSERT INTO media (url,type,mime,last_status,last_check,owner) VALUES (SELECT %s,%s,%s,200,now(),%s WHERE NOT EXISTS (SELECT 1 FROM media WHERE url=%s))", (url,typ,detected_mime, config.IDB_UUID", url))
    #     current_app.config["DB"]._cur.execute("INSERT INTO objects (bucket, etag, detected_mime) (SELECT %s, %s, %s WHERE NOT EXISTS (SELECT 1 FROM objects WHERE etag=%s))", (typ, h, detected_mime, h))
    #     current_app.config["DB"]._cur.execute("INSERT INTO media_objects (url, etag) VALUES (%s,%s)", (url,h))
