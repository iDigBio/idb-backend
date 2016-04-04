from gevent.pool import Pool
from gevent import monkey

monkey.patch_all()

from idb.postgres_backend import apidbpool
from idb.helpers.storage import IDigBioStorage
from idb.helpers.media_validation import get_validator
from idb.helpers.conversions import get_accessuri, get_media_type

import magic
import os
import requests
from requests.auth import HTTPBasicAuth
import traceback
import datetime

from psycopg2.extensions import cursor

s = requests.Session()
adapter = requests.adapters.HTTPAdapter(pool_connections=100, pool_maxsize=100)
s.mount('http://', adapter)
s.mount('https://', adapter)

auth = HTTPBasicAuth(os.environ.get("IDB_UUID"), os.environ.get("IDB_APIKEY"))


def create_schema():
    with apidbpool.cursor() as cur:
        cur.execute("BEGIN")
        cur.execute("""CREATE TABLE IF NOT EXISTS media (
            id BIGSERIAL PRIMARY KEY,
            url text UNIQUE,
            type varchar(20),
            mime varchar(255),
            last_status integer,
            last_check timestamp
        )
        """)
        cur.execute("""CREATE TABLE IF NOT EXISTS objects (
            id BIGSERIAL PRIMARY KEY,
            bucket varchar(255) NOT NULL,
            etag varchar(41) NOT NULL UNIQUE,
            detected_mime varchar(255),
            derivatives boolean DEFAULT false
        )
        """)
        cur.execute("""CREATE TABLE IF NOT EXISTS media_objects (
            id BIGSERIAL PRIMARY KEY,
            url text NOT NULL REFERENCES media(url),
            etag varchar(41) NOT NULL REFERENCES objects(etag),
            modified timestamp NOT NULL DEFAULT now()
        )
        """)


ignore_prefix = [
    "http://media.idigbio.org/",
    "http://firuta.huh.harvard.edu/",
    "http://www.tropicos.org/"
]

user_agent = {'User-Agent': 'iDigBio Media Ingestor (idigbio@acis.ufl.edu https://www.idigbio.org/wiki/index.php/Media_Ingestor)'}

def get_media(tup, cache_bad=False):
    url, t, fmt = tup

    url_path = "bad_media/" + url.replace("/", "^^")

    media_status = 1000
    apiimg_post_status = 0

    try:
        for p in ignore_prefix:
            if url.startswith(p):
                apidbpool.execute(
                    "UPDATE media SET last_status=%s, last_check=now() WHERE url=%s",
                    (1002, url))
                print "Skip", url, t, fmt, p
                return False

        media_req = s.get(url, headers = user_agent)
        media_status = media_req.status_code
        media_req.raise_for_status()

        validator = get_validator(fmt)
        valid, detected_mime = validator(url, t, fmt, media_req.content)
        if valid:
            print datetime.datetime.now(), "Validated Media:", url, t, fmt, detected_mime
            apiimg_req = s.post("http://media.idigbio.org/upload/" + t,
                                data={"filereference": url},
                                files={'file': media_req.content},
                                auth=auth)
            apiimg_post_status = apiimg_req.status_code
            apiimg_req.raise_for_status()
            apiimg_o = apiimg_req.json()
            with apidbpool.cursor() as cur:
                cur.execute("UPDATE media SET last_status=%s, last_check=now() WHERE url=%s",
                            (200, url))
                cur.execute("""INSERT INTO objects (etag, bucket, detected_mime)
                                   SELECT %(etag)s, %(type)s, %(mime)s
                                   WHERE NOT EXISTS (SELECT 1 FROM objects WHERE etag=%(etag)s)""",
                            {"etag": apiimg_o["file_md5"], "type": t, "mime": detected_mime})
                cur.execute("INSERT INTO media_objects (url,etag) VALUES (%s,%s)",
                            (url, apiimg_o["file_md5"]))
            return True
        else:
            apidbpool.execute(
                "UPDATE media SET last_status=%s, last_check=now() WHERE url=%s",
                (1001, url))
            if cache_bad:
                with open(url_path, "wb") as outf:
                    outf.write(media_req.content)
            print datetime.datetime.now(), "Failure", url, t, valid, fmt, detected_mime
            return False
    except KeyboardInterrupt as e:
        raise e
    except:
        if apiimg_post_status > 200:
            # had a problem posting valid media, set status code at 2000 + the actual status code.
            sql = ("UPDATE media SET last_status=%s, last_check=now() WHERE url=%s",
                   (apiimg_post_status+2000, url))
        else:
            sql = ("UPDATE media SET last_status=%s, last_check=now() WHERE url=%s",
                   (media_status, url))
        apidbpool.execute(*sql)
        print url, t, fmt, "GET media status:", media_status, "POST media status:", apiimg_post_status
        traceback.print_exc()
        return False


def write_urls_to_db(media_urls):
    print "Start Inserts"
    inserted_urls = set()

    scanned = 0
    to_insert, to_update = [], []
    itersql = """SELECT type,data
                 FROM idigbio_uuids_data
                 WHERE type='mediarecord' and deleted=false"""

    for r in apidbpool.fetchiter(itersql, named=True):
        scanned += 1
        url = get_accessuri(r["type"], r["data"])["accessuri"]
        o = get_media_type(r["type"], r["data"])
        form = o["format"]
        t = o["mediatype"]

        if url is not None:
            url = url.replace("&amp;", "&").strip()

            for p in ignore_prefix:
                if url.startswith(p):
                    break
            else:
                if url in media_urls:
                    # We're going to change something, but only if we're
                    # adding/replacing things, not nulling existing values.
                    if not (t, form) == media_urls[url] and form is not None and (t is not None or media_urls[url][0] is None):
                        to_update.append((t, form, url))
                elif url not in inserted_urls:
                    to_insert.append((url, t, form))
                    inserted_urls.add(url)

        if scanned % 100000 == 0:
            print len(to_insert), len(to_update), scanned
    with apidbpool.cursor() as cur:
        cur.executemany("INSERT INTO media (url,type,mime) VALUES (%s,%s,%s)", to_insert)
        cur.executemany("UPDATE media SET type=%s, mime=%s, last_status=NULL, last_check=NULL WHERE url=%s",
                        to_update)

    print len(to_insert), len(to_update), scanned


def get_postgres_media_urls():
    media_urls = dict()
    print "Get Media URLs"
    sql = "SELECT url,type,mime FROM media"
    for r in apidbpool.fetchiter(sql, cursor_factory=cursor):
        media_urls[r[0]] = (r[1], r[2])
    return media_urls


def get_postgres_media_objects():
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
                    print count, rowcount
                    lrc = rowcount
    insertconn.commit()
    print count, rowcount


def get_objects_from_ceph():
    existing_objects = set(
        r[0] for r in apidbpool.fetchiter("SELECT etag FROM objects", cursor_factory=cursor))

    print len(existing_objects)

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
                            print "Ceph Error", b_k, k.name
                    count += 1

                    if rowcount != lrc and rowcount % 10000 == 0:
                        print count, rowcount
                        conn.commit()
                        lrc = rowcount
                print count, rowcount
                conn.commit()

def set_deriv_from_ceph():
    s = IDigBioStorage()
    b = s.get_bucket("idigbio-images-prod-thumbnail")
    count = 0
    with apidbpool.connection() as conn:
        with apidbpool.cursor() as cur:
            for k in b.list():
                cur.execute("UPDATE objects SET derivatives=true WHERE etag=%s", (k.name.split(".")[0],))
                count += 1

                if count % 10000 == 0:
                    print count
                    conn.commit()
            print count
            conn.commit()

def get_media_generator():
    sql = """SELECT * FROM (
        SELECT substring(url from 'https?://[^/]*[/?]'), count(*)
        FROM (
            SELECT media.url, media_objects.etag
            FROM media
            LEFT JOIN media_objects ON media.url = media_objects.url
            WHERE type IS NOT NULL
              AND (last_status IS NULL or (last_status >= 400 and last_check < now() - '1 month'::interval))
        ) AS a
        WHERE a.etag IS NULL GROUP BY substring(url from 'https?://[^/]*[/?]')
    ) AS b WHERE substring != '' ORDER BY count"""
    subs_rows = apidbpool.fetchall(sql)
    for sub_row in subs_rows:
        subs = sub_row[0]
        sql = ("""SELECT url,type,mime
            FROM (
               SELECT media.url,type,mime,etag
               FROM media
               LEFT JOIN media_objects ON media.url = media_objects.url
               WHERE media.url LIKE %s
                 AND type IS NOT NULL
                 AND (last_status IS NULL
                      OR (last_status >= 400 AND last_check < now() - '1 month'::interval))
               ) AS a
            WHERE a.etag IS NULL""", (subs + "%",))
        url_rows = apidbpool.fetchall(*sql)
        for url_row in url_rows:
            yield tuple(url_row[0:3])

def get_media_consumer():
    p = Pool(5)
    count = 0
    t = 0
    f = 0
    for r in p.imap_unordered(get_media, get_media_generator()):
        if r:
            t += 1
        else:
            f += 1
        count += 1

        if count % 10000 == 0:
            print count, t, f
    print count, t, f

def main():
    import sys
    #create_schema()

    if len(sys.argv) > 1 and sys.argv[1] == "get_media":
        print "Starting get_media operations at ", datetime.datetime.now()
        get_media_consumer()
        print "Finished get_media operations at ", datetime.datetime.now()
    else:
        print "Starting media_urls operations at ", datetime.datetime.now()
        media_urls = get_postgres_media_urls()
        write_urls_to_db(media_urls)
        # get_objects_from_ceph()
        # get_postgres_media_objects()
        #set_deriv_from_ceph()
        print "Finished media_urls operations at ", datetime.datetime.now()

if __name__ == '__main__':
    main()

# SQL Queries to import from old table:
#insert into media (url,type,owner) (select lookup_key,type,user_uuid::uuid from (select media.url, idb_object_keys.lookup_key, idb_object_keys.type, idb_object_keys.user_uuid from idb_object_keys left join media on lookup_key=url) as a where url is null);
#insert into objects (bucket,etag) (select type,etag from (select lookup_key, type, a.etag, b.etag as n from idb_object_keys as a left join objects as b on a.etag=b.etag) as c where n is null);
#insert into media_objects (url,etag,modified) (select lookup_key,etag,date_modified from (select media_objects.url, idb_object_keys.lookup_key, idb_object_keys.etag, idb_object_keys.date_modified from idb_object_keys left join media_objects on lookup_key=url and media_objects.etag=idb_object_keys.etag) as a where url is null);
