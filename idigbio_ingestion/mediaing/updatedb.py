from __future__ import division, absolute_import, print_function

from psycopg2.extensions import cursor


from idb.postgres_backend import apidbpool
from idb.helpers.storage import IDigBioStorage
from idb.helpers.conversions import get_accessuri, get_media_type
from idb.helpers.logging import idblogger


logger = idblogger.getChild('mediaing')


def updatedb(prefix=None, since=None):
    """Runs the process of finding new urls

    Records that are imported don't go directly into the media table,
    instead periodically we need to run this to look for new urls in
    mediarecords data.

    """
    media_urls = existing_media_urls(prefix)
    to_insert, to_update = find_new_urls(media_urls, prefix, since)
    write_urls_to_db(to_insert, to_update)


def daily(prefix=None):
    "Run the daily update; i.e. look for mediarecords with a modified in the last day"
    from datetime import datetime, timedelta
    since = datetime.now() - timedelta(hours=-25)
    updatedb(prefix=prefix, since=since)


def existing_media_urls(prefix=None):
    "Find existing media urls"
    logger.info("Get Media URLs, prefix: %r", prefix)
    sql = "SELECT url,type,mime FROM media"
    params = []
    if prefix:
        sql += " WHERE url LIKE %s"
        params.append(prefix + '%')

    rows = apidbpool.fetchiter(sql, params, cursor_factory=cursor)
    media_urls = {r[0]: (r[1], r[2]) for r in rows}
    logger.info("Found %d urls already in DB", len(media_urls))
    return media_urls

def find_new_urls(media_urls, prefix=None, since=None):
    """Iterate through mediarecords' urls and ensure they are in existing urls"""
    logger.info("Searching for new URLs")

    scanned = 0
    to_insert = {}   # prevent duplication
    to_update = []   # just accumulate
    itersql = """SELECT data
                 FROM idigbio_uuids_data
                 WHERE type='mediarecord' AND deleted=false"""
    params = []
    if since:
        logger.debug("Filtering mediarecords modified > %s", since)
        itersql += "\n AND modified > %s"
        params.append(since)

    results = apidbpool.fetchiter(itersql, params, name='write_urls_to_db', cursor_factory=cursor)
    for row in results:
        data = row[0]
        if scanned % 100000 == 0:
            logger.info("Inserting: %8d, Updating: %8d, Scanned: %8d",
                        len(to_insert), len(to_update), scanned)

        scanned += 1
        url = get_accessuri('mediarecord', data)["accessuri"]
        if url is None:
            continue
        url = url.replace("&amp;", "&").strip()
        if prefix and not url.startswith(prefix):
            continue

        o = get_media_type('mediarecord', data)
        t, mime = o["mediatype"], o["format"]

        entry = media_urls.get(url)
        if entry:
            # We're going to change something, but only if we're
            # adding/replacing things, not nulling existing values.
            if (t, mime) != entry and mime and (t or entry[0] is None):
                to_update.append((t, mime, url))
        elif url not in to_insert:
            to_insert[url] = (t, mime)
        else:
            logger.debug("Repeated insert from ")

    logger.info("Inserting: %8d, Updating: %8d, Scanned: %8d; Finished Scan",
                len(to_insert), len(to_update), scanned)

    return to_insert, to_update


def write_urls_to_db(to_insert, to_update):
    with apidbpool.cursor(autocommit=True) as cur:
        inserted = cur.executemany(
            "INSERT INTO media (url,type,mime) VALUES (%s,%s,%s)",
            ((k, v[0], v[1]) for k,v in to_insert.items()))
        updated = cur.executemany(
            "UPDATE media SET type=%s, mime=%s, last_status=NULL, last_check=NULL WHERE url=%s",
            to_update)
    logger.info("Inserted : %8d, Updated : %8d", inserted, updated)


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
