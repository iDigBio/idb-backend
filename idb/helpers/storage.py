# -*- coding: utf-8 -*-
"""
Python-2.7-compatible rewrite of the iDigBio S3 helper using **boto3**.

Differences from the Python-3 version:
* No function/variable annotations or f-strings.
* Avoids keyword-only parameters and other Py-3 syntax.
* Uses `str.format()` for string interpolation.

The public surface mirrors the legacy boto-v2 class so callers need
minimal changes apart from the import path.
"""

from __future__ import absolute_import, division, print_function, unicode_literals

import io
import math
import os
import time

import boto3

# ------------------------------------------------------------------
# Compatibility helpers (Py2/Py3 string detection)
# ------------------------------------------------------------------
try:
    # Python 2.x
    _string_types = (basestring,)  # type: ignore  # noqa: F821
except NameError:  # Python 3.x / PyPy3
    _string_types = (str, bytes)

from botocore.config import Config
from botocore.exceptions import ClientError, BotoCoreError

from idb import config
from idb.helpers.logging import idblogger
from idb.postgres_backend.db import MediaObject

logger = idblogger.getChild('storage')

PRIVATE_BUCKETS = set(['debugfile'])


class IDigBioStorage(object):

    MAX_CHUNK_SIZE = 1024 ** 3  # 1 GiB

    def __init__(self, host=None, access_key=None, secret_key=None):
        host = host or config.IDB_STORAGE_HOST
        access_key = access_key or config.IDB_STORAGE_ACCESS_KEY
        secret_key = secret_key or config.IDB_STORAGE_SECRET_KEY

        if not (host and access_key and secret_key):
            raise ValueError('host, access_key and secret_key must be provided')

        # Build endpoint URL; allow optional host:port form
        if ':' in host:
            host_only, port = host.split(':', 1)
            endpoint_url = 'http://{0}:{1}'.format(host_only, port)
        else:
            endpoint_url = 'http://{0}'.format(host)

        session = boto3.session.Session()
        kwargs = dict(
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
            endpoint_url=endpoint_url,
            use_ssl=False,
            config=Config(s3={'addressing_style': 'path'}),
        )
        self._client = session.client('s3', **kwargs)
        self._resource = session.resource('s3', **kwargs)
        logger.debug('Initialised IDigBioStorage connection to %s', endpoint_url)

    # ------------------------------------------------------------------
    # Convenience wrappers
    # ------------------------------------------------------------------
    def get_bucket(self, bucket_name):
        return self._resource.Bucket(bucket_name)

    def get_key(self, key_name, bucket_name, bucket=None):
        if bucket is None:
            bucket = self.get_bucket(bucket_name)
        return bucket.Object(key_name)

    # ------------------------------------------------------------------
    # URL helper
    # ------------------------------------------------------------------
    def get_link(self, key_name, bucket_name, secure=False):
        scheme = 'https' if secure else 'http'
        return '{0}://{1}/{2}/{3}'.format(scheme, config.IDB_STORAGE_HOST, bucket_name, key_name)

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------
    def _stream_size(self, fp):
        pos = fp.tell()
        fp.seek(0, os.SEEK_END)
        size = fp.tell()
        fp.seek(pos, os.SEEK_SET)
        return size

    # ------------------------------------------------------------------
    # Upload API
    # ------------------------------------------------------------------
    def upload(self, obj, fobj, content_type=None, public=None, md5=None, multipart='auto', size=None):
        if public is None:
            public = obj.bucket_name not in PRIVATE_BUCKETS

        # Normalise *fobj* to a file-like object in binary mode
        if isinstance(fobj, _string_types):
            fobj = open(fobj, 'rb')
        elif hasattr(fobj, 'open') and not hasattr(fobj, 'read'):
            fobj = fobj.open('rb')
        elif not hasattr(fobj, 'read'):
            raise ValueError('Unsupported fobj type: {0}'.format(type(fobj)))

        if size is None:
            size = self._stream_size(fobj)

        extra_args = {}
        if content_type:
            extra_args['ContentType'] = content_type
        if public:
            extra_args['ACL'] = 'public-read'

        if multipart == 'auto' and size > self.MAX_CHUNK_SIZE:
            return self._upload_multipart(obj, fobj, size, extra_args)

        def _single():
            self._client.upload_fileobj(fobj, obj.bucket_name, obj.key, ExtraArgs=extra_args)
        self.retry_loop(_single)

        if md5:
            etag = self._client.head_object(Bucket=obj.bucket_name, Key=obj.key)['ETag'].strip('"')
            if etag != md5:
                raise ValueError('MD5 mismatch: remote {0} vs local {1}'.format(etag, md5))
        return obj

    def _upload_multipart(self, obj, fobj, size, extra_args):
        part_count = int(math.ceil(float(size) / self.MAX_CHUNK_SIZE))
        logger.debug('Multipart upload %s in %d parts', obj.key, part_count)

        mpu = self._client.create_multipart_upload(Bucket=obj.bucket_name, Key=obj.key, **extra_args)
        upload_id = mpu['UploadId']
        parts = []

        try:
            for idx in range(part_count):
                offset = idx * self.MAX_CHUNK_SIZE
                part_size = min(self.MAX_CHUNK_SIZE, size - offset)
                fobj.seek(offset)
                body = fobj.read(part_size)

                def _one_part():
                    resp = self._client.upload_part(
                        Bucket=obj.bucket_name,
                        Key=obj.key,
                        PartNumber=idx + 1,
                        UploadId=upload_id,
                        Body=body,
                    )
                    parts.append({'PartNumber': idx + 1, 'ETag': resp['ETag']})
                self.retry_loop(_one_part)

            self._client.complete_multipart_upload(
                Bucket=obj.bucket_name,
                Key=obj.key,
                UploadId=upload_id,
                MultipartUpload={'Parts': parts},
            )
        except Exception:
            logger.exception('Aborting multipart upload')
            self._client.abort_multipart_upload(Bucket=obj.bucket_name, Key=obj.key, UploadId=upload_id)
            raise
        return obj

    # ------------------------------------------------------------------
    # Retry helper
    # ------------------------------------------------------------------
    @staticmethod
    def retry_loop(func, retries=3):
        attempt = 1
        while True:
            try:
                return func()
            except (ClientError, BotoCoreError):
                logger.exception('Storage operation failed (%d/%d)', attempt, retries)
                attempt += 1
                if attempt > retries:
                    raise
                time.sleep(2 ** (attempt + 1))

    # ------------------------------------------------------------------
    # Download helpers
    # ------------------------------------------------------------------
    def get_contents_to_filename(self, obj, filename, md5=None):
        """Download *obj* to *filename* atomically and (optionally) verify MD5.

        The file is first written to *filename*.tmp; if MD5 verification
        fails the temp file is deleted and the original path is left
        untouched – mirroring the legacy AtomicFile behaviour the tests
        expect.
        """
        temp_name = filename + '.tmp'
        try:
            with open(temp_name, 'wb') as fh:
                self._client.download_fileobj(obj.bucket_name, obj.key, fh)
            # --------------------------------------------------
            # Verify integrity
            # --------------------------------------------------
            if md5:
                # For multipart objects the ETag is not a plain MD5; instead
                # calculate the digest of the temp file so the caller can
                # still rely on integrity checking just like the legacy boto
                # wrapper did.
                def _digest(path):
                    import hashlib
                    h = hashlib.md5()
                    with open(path, 'rb') as fp:
                        for chunk in iter(lambda: fp.read(8192), b''):
                            h.update(chunk)
                    return h.hexdigest()

                local_md5 = _digest(temp_name)
                if local_md5 != md5:
                    raise ValueError('MD5 mismatch: remote bytes digest {0} vs expected {1}'.format(local_md5, md5))

            # Passed verification → atomic move into place
            if os.path.exists(filename):
                os.unlink(filename)
            os.rename(temp_name, filename)
        finally:
            # Clean up temp file if we bailed early
            if os.path.exists(temp_name):
                try:
                    os.unlink(temp_name)
                except OSError:
                    pass
        return filename

    def get_contents_to_mem(self, obj, md5=None):
        buff = io.BytesIO()
        self._client.download_fileobj(obj.bucket_name, obj.key, buff)
        if md5:
            etag = self._client.head_object(Bucket=obj.bucket_name, Key=obj.key)['ETag'].strip('"')
            if etag != md5:
                raise ValueError('MD5 mismatch: remote {0} vs expected {1}'.format(etag, md5))
        buff.seek(0)
        return buff

    # ------------------------------------------------------------------
    # MediaObject helpers
    # ------------------------------------------------------------------
    def get_key_by_url(self, url, idbmodel=None):
        mo = MediaObject.fromurl(url, idbmodel)
        if mo is None:
            raise ValueError('No media with url {0!r}'.format(url))
        return self.get_key(mo.keyname, mo.bucketname)

    def get_file_by_url(self, url, file_name=None):
        obj = self.get_key_by_url(url)
        if file_name is None:
            file_name = os.path.basename(obj.key)
        return self.get_contents_to_filename(obj, file_name)

    def fetch(self, key_name, bucket_name, filename, md5=None):
        obj = self.get_key(key_name, bucket_name)
        return self.get_contents_to_filename(obj, filename, md5)
