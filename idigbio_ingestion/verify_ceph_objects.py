"""This script verifies that objects from the db are actually in ceph


https://www.idigbio.org/redmine/issues/1863
"""



import io
import logging
from collections import namedtuple, Counter


from gevent.pool import Pool
from gevent import monkey
from psycopg2.extensions import cursor

from idb.helpers.storage import IDigBioStorage
from idb.postgres_backend import apidbpool
from idb.postgres_backend.db import MediaObject
from idb.config import logger


from boto.exception import S3ResponseError

CheckResult = namedtuple('CheckResult', ['mo', 'exists'])


def get_all_objects(limit=None):
    sql = """
        SELECT objects.bucket, objects.etag
        FROM objects
    """
    if limit:
        sql += "limit " + str(limit)

    for r in apidbpool.fetchiter(sql, cursor_factory=cursor, named=True):
        yield MediaObject(bucket=r[0], etag=r[1])


STORE = IDigBioStorage()


def check_object(mo, store=STORE):
    try:
        k = store.get_key(mo.keyname, mo.bucketname)
        return CheckResult(mo, k.exists())
    except S3ResponseError as e:
        # WARNING: Exception.message removed in Python 3 (exception-message-attribute)
        return CheckResult(mo, e.message)
    except:
        logger.exception("Failed on %s", mo.etag)


def log_counts(results, filename='bad-etags.csv', errfilename='err-etags.csv'):
    counts = Counter()
    with io.open(errfilename, 'w', encoding='utf-8') as ferr:
        with io.open(filename, 'w', encoding='utf-8') as f:
            ferr.write('etag,bucketname,message')
            f.write('etag,bucketname')
            for idx, cr in enumerate(results, 1):
                counts[cr.exists] += 1
                if cr.exists is False:
                    f.write("{0},{1}\n".format(cr.mo.etag, cr.mo.bucketname))
                elif cr.exists is True:
                    pass
                else:
                    ferr.write("{0},{1},{2}\n".format(cr.mo.etag, cr.mo.bucketname, cr.exists))

                if idx % 25000 == 0:
                    logger.info("Count: %8d; existed: %8d, missing: %8d",
                                idx, counts[True], counts[False])
                yield cr
    logger.info("Count: %8d; existed: %8d, missing: %8d %r",
                idx, counts[True], counts[False], counts.most_common())


def reverify(media_objects, poolsize=50):
    p = Pool(poolsize)
    check_results = p.imap_unordered(check_object, media_objects)
    for cr in check_results:
        mo = cr.mo
        if cr.exists:
            logger.warning("Now exists: %s in %s", mo.etag, mo.bucket)
            continue
        yield cr.mo

def delete_etags(media_objects):
    sql = """
        DELETE FROM media_objects where etag = %(etag)s;
        DELETE FROM objects WHERE etag = %(etag)s;
    """
    c = apidbpool.executemany(sql, ({'etag': mo.etag} for mo in media_objects))
    logger.info("DELETEd %s media_objects and objects", c)


def main(poolsize=50, limit=5000):
    p = Pool(poolsize)
    try:
        objects = get_all_objects(limit)
        check_results = p.imap_unordered(check_object, objects)
        check_results = log_counts(check_results)
        for _ in check_results:
            pass
    except KeyboardInterrupt:
        p.kill()
        raise

if __name__ == '__main__':
    monkey.patch_all()
    logging.root.setLevel(logging.DEBUG)
    #logging.root.setLevel(logging.INFO)
    logging.getLogger('boto').setLevel(logging.WARNING)
    logging.getLogger('requests').setLevel(logging.WARNING)
    main(limit=None)
