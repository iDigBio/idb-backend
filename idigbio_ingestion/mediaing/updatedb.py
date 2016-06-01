from __future__ import division, absolute_import, print_function

from psycopg2.extensions import cursor


from idb.postgres_backend import apidbpool
from idb.helpers.storage import IDigBioStorage
from idb.helpers.conversions import get_accessuri, get_media_type
from idb.helpers.logging import idblogger

from . import check_ignore_media


logger = idblogger.getChild('mediaing')

def updatedb(prefix=None):
    """Runs the process of finding new urls

    Records that are imported don't go directly into the media table,
    instead periodically we need to run this to look for new urls in
    mediarecords data.

    """
    media_urls = existing_media_urls(prefix)
    write_urls_to_db(media_urls, prefix)
    # get_objects_from_ceph()
    # get_postgres_media_objects(urlfilter)


def existing_media_urls(prefix=None):
    "Find existing media urls"
    media_urls = dict()
    logger.info("Get Media URLs, prefix: %r", prefix)
    sql = "SELECT url,type,mime FROM media"
    params = []
    if prefix:
        sql += " WHERE url LIKE %s"
        params.append(prefix + '%')

    for r in apidbpool.fetchiter(sql, params, cursor_factory=cursor):
        media_urls[r[0]] = (r[1], r[2])
    logger.info("Found %d urls already in DB", len(media_urls))
    return media_urls


def write_urls_to_db(media_urls, prefix=None):
    """Iterate through mediarecords' urls and ensure they are in existing urls"""
    logger.info("Searching for new URLs")

    scanned = 0
    to_insert = {}   # prevent duplication
    to_update = []   # just accumulate
    itersql = """SELECT type,data
                 FROM idigbio_uuids_data
                 WHERE type='mediarecord' and deleted=false"""

    for type, data in apidbpool.fetchiter(itersql, named=True, cursor_factory=cursor):
        if scanned % 100000 == 0:
            logger.info("Inserting: %8d, Updating: %8d, Scanned: %8d",
                        len(to_insert), len(to_update), scanned)

        scanned += 1
        url = get_accessuri(type, data)["accessuri"]
        if url is None:
            continue
        url = url.replace("&amp;", "&").strip()
        if check_ignore_media(url) or (prefix and not url.startswith(prefix)):
            continue

        o = get_media_type(type, data)
        form = o["format"]
        t = o["mediatype"]

        if url in media_urls:
            # We're going to change something, but only if we're
            # adding/replacing things, not nulling existing values.
            if not (t, form) == media_urls[url] and form is not None and (t is not None or media_urls[url][0] is None):
                to_update.append((t, form, url))
        elif url not in to_insert:
            to_insert[url] = (t, form)

    logger.info("Inserting: %8d, Updating: %8d, Scanned: %8d; Finished loop, processing DB",
                len(to_insert), len(to_update), scanned)

    with apidbpool.cursor() as cur:
        cur.executemany("INSERT INTO media (url,type,mime) VALUES (%s,%s,%s)",
                        ((k, v[0], v[1]) for k,v in to_insert.items()))
        cur.executemany("UPDATE media SET type=%s, mime=%s, last_status=NULL, last_check=NULL WHERE url=%s",
                        to_update)

    logger.info("Inserted : %8d, Updated : %8d, Scanned: %8d (Finished)",
                len(to_insert), len(to_update), scanned)


def get_postgres_media_objects(prefix):
    assert prefix is None, "prefix isn't implemented on this function"
    count, rowcount, lrc = 0, 0, 0
    sql = "SELECT lookup_key, etag, date_modified FROM idb_object_keys"
    with apidbpool.connection() as insertconn:
        with insertconn.cursor(cursor_factory=cursor) as cur:
            for r in apidbpool.fetchiter(sql, name="get_postgres_media_objects"):
                cur.execute("""
                     INSERT INTO media_objects (url, etag, modified)
                     SELECT %(url)s, %(etag)s, %(modified)s
                     WHERE EXISTS (SELECT 1 FROM media WHERE url=%(url)s)
                       AND EXISTS (SELECT 1 FROM objects WHERE etag=%(etag)s)
                       AND NOT EXISTS (SELECT 1 FROM media_objects WHERE url=%(url)s AND etag=%(etag)s)
                """, {"url": r[0], "etag": r[1], "modified": r[2]})
                count += 1
                rowcount += cur.rowcount

                if rowcount != lrc and rowcount % 10000 == 0:
                    insertconn.commit()
                    logger.info("Count: %8d,  rowcount: %8d", count, rowcount)
                    lrc = rowcount
    insertconn.commit()
    logger.info("Count: %8d,  rowcount: %8d", count, rowcount)


def get_objects_from_ceph():
    import magic
    existing_objects = set(
        r[0] for r in apidbpool.fetchiter("SELECT etag FROM objects", cursor_factory=cursor))

    logger.info("Found %d objects", len(existing_objects))

    s = IDigBioStorage()
    buckets = ["datasets", "images"]
    count = 0
    rowcount = 0
    lrc = 0
    with apidbpool.connection() as conn:
        with apidbpool.cursor() as cur:
            for b_k in buckets:
                b = s.get_bucket("idigbio-" + b_k + "-prod")
                for k in b.list():
                    if k.name not in existing_objects:
                        try:
                            ks = k.get_contents_as_string(headers={'Range': 'bytes=0-100'})
                            detected_mime = magic.from_buffer(ks, mime=True)
                            cur.execute(
                                """INSERT INTO objects (bucket,etag,detected_mime)
                                   SELECT %(bucket)s,%(etag)s,%(dm)s
                                   WHERE NOT EXISTS(
                                      SELECT 1 FROM objects WHERE etag=%(etag)s)""",
                                {"bucket": b_k, "etag": k.name, "dm": detected_mime})
                            existing_objects.add(k.name)
                            rowcount += cur.rowcount
                        except:
                            logger.exception("Ceph Error; bucket:%s keyname:%s", b_k, k.name)
                    count += 1

                    if rowcount != lrc and rowcount % 10000 == 0:
                        logger.info("Count: %8d,  rowcount: %8d", count, rowcount)

                        conn.commit()
                        lrc = rowcount
                conn.commit()
                logger.info("Count: %8d,  rowcount: %8d  (Finished %s)", count, rowcount, b_k)
