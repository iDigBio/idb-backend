from gevent.pool import Pool
from gevent import monkey

monkey.patch_all()

from idb.postgres_backend.db import PostgresDB
from idb.helpers.storage import IDigBioStorage

import psycopg2
import magic
import os
import requests
from requests.auth import HTTPBasicAuth
import traceback
import json

s = requests.Session()
adapter = requests.adapters.HTTPAdapter(pool_connections=100, pool_maxsize=100)
s.mount('http://', adapter)
s.mount('https://', adapter)

auth = HTTPBasicAuth(os.environ.get("IDB_UUID"), os.environ.get("IDB_APIKEY"))

db = PostgresDB()
local_pg = psycopg2.connect(user="godfoder")
local_cur = local_pg.cursor()

# Set a mime type to none to explicitly ignore it
mime_mapping = {
    "image/jpeg": "images",
    "text/html": None,
    "image/dng": None,
    "application/xml": None,
    "image/x-adobe-dng": None,
    "audio/mpeg3": None,
    "text/html": None,
    None: None
}


def create_schema():
    local_cur.execute("BEGIN")
    local_cur.execute("""CREATE TABLE IF NOT EXISTS media (
        id BIGSERIAL PRIMARY KEY,
        url text UNIQUE,
        type varchar(20),
        mime varchar(255),
        last_status integer,
        last_check timestamp
    )
    """)
    local_cur.execute("""CREATE TABLE IF NOT EXISTS objects (
        id BIGSERIAL PRIMARY KEY,
        bucket varchar(255) NOT NULL,
        etag varchar(41) NOT NULL UNIQUE,
        detected_mime varchar(255),
        derivatives boolean DEFAULT false
    )
    """)
    local_cur.execute("""CREATE TABLE IF NOT EXISTS media_objects (
        id BIGSERIAL PRIMARY KEY,
        url text NOT NULL REFERENCES media(url),
        etag varchar(41) NOT NULL REFERENCES objects(etag),
        modified timestamp NOT NULL DEFAULT now()
    )
    """)
    local_pg.commit()

ignore_prefix = [
    "http://media.idigbio.org/",
    "http://firuta.huh.harvard.edu/"
]

format_validators = {
    "model/mesh": lambda url, t, fmt, content: (url.endswith(".stl"), "model/mesh")
}

def default_format_validator(url, t, fmt, content):
    mime = magic.from_buffer(content, mime=True)
    return (fmt == mime, mime)

def get_media(tup, cache_bad=False):
    url, t, fmt = tup

    url_path = "bad_media/"+url.replace("/","^^")

    media_status = 1000

    try:
        for p in ignore_prefix:
            if url.startswith(p):
                local_cur.execute("UPDATE media SET last_status=%s, last_check=now() WHERE url=%s", (1002,url))
                print "Skip", url, t, fmt, p
                return False

        media_req = s.get(url)
        media_status = media_req.status_code
        media_req.raise_for_status()

        if fmt in format_validators:
            validator = format_validators[fmt]
        else:
            validator = default_format_validator

        valid, detected_mime = validator(url,t,fmt,media_req.content)
        if valid:
            print "Success", url, t, fmt, detected_mime
            apiimg_req = s.post("http://media.idigbio.org/upload/" + t, data={"filereference": url}, files={'file': media_req.content }, auth=auth)
            apiimg_req.raise_for_status()
            apiimg_o = apiimg_req.json()
            local_cur.execute("INSERT INTO objects (etag,bucket,detected_mime) SELECT %(etag)s, %(type)s, %(mime)s WHERE NOT EXISTS (SELECT 1 FROM objects WHERE etag=%(etag)s)", {"etag": apiimg_o["file_md5"], "type": t, "mime": detected_mime})
            local_cur.execute("INSERT INTO media_objects (url,etag) VALUES (%s,%s)", (url,apiimg_o["file_md5"]))
            local_pg.commit()
            return True
        else:
            local_cur.execute("UPDATE media SET last_status=%s, last_check=now() WHERE url=%s", (1001,url))
            local_pg.commit()
            if cache_bad:
                with open(url_path,"wb") as outf:
                    outf.write(media_req.content)
            print "Failure", url, t, valid, fmt, detected_mime
            return False
    except KeyboardInterrupt as e:
        raise e
    except:
        local_pg.rollback()
        local_cur.execute("UPDATE media SET last_status=%s, last_check=now() WHERE url=%s", (media_status, url))
        local_pg.commit()
        print url, t, fmt, media_status
        traceback.print_exc()
        return False


def write_urls_to_db(media_urls):
    print "Start Inserts"

    inserted_urls = set()

    inserts = 0
    scanned = 0
    total_inserts = 0
    cur = db._get_ss_cursor()
    cur.execute(
        "SELECT data FROM idigbio_uuids_data WHERE type='mediarecord' and deleted=false")
    local_cur.execute("BEGIN")
    with open("url_out.json_null", "wb") as outf:
        for r in cur:
            scanned += 1

            url = None
            if "ac:accessURI" in r["data"]:
                url = r["data"]["ac:accessURI"]
            elif "ac:bestQualityAccessURI" in r["data"]:
                url = r["data"]["ac:bestQualityAccessURI"]
            else:
                # Don't use identifier as a url for things that supply audubon core properties
                for k in r["data"].keys():
                    if k.startswith("ac:"):
                        break
                else:
                    if "dcterms:identifier" in r["data"]:
                        url = r["data"]["dcterms:identifier"]
                    elif "dc:identifier" in r["data"]:
                        url = r["data"]["dc:identifier"]

            form = None
            if "dcterms:format" in r["data"]:
                form = r["data"]["dcterms:format"].strip()
            elif "dc:format" in r["data"]:
                form = r["data"]["dc:format"].strip()

            t = None
            if form in mime_mapping:
                t = mime_mapping[form]

            if url is not None:
                url = url.replace("&amp;", "&").strip()

                for p in ignore_prefix:
                    if url.startswith(p):
                        break
                else:
                    if url not in media_urls and url not in inserted_urls:
                        local_cur.execute("INSERT INTO media (url,type,mime) VALUES (%s,%s,%s)", (url, t, form))
                        inserts += 1
                        inserted_urls.add(url)                        

            if inserts >= 10000:
                local_pg.commit()
                local_cur.execute("BEGIN")
                total_inserts += inserts
                print total_inserts, scanned
                inserts = 0
    local_pg.commit()
    total_inserts += inserts
    print total_inserts, scanned


def get_postgres_media_urls():
    media_urls = set()

    print "Get Media URLs"

    local_cur = local_pg.cursor()
    local_cur.execute("SELECT url FROM media")
    for r in local_cur:
        media_urls.add(r[0])

    return media_urls

def get_postgres_media_objects():
    cur = db._get_ss_cursor()
    cur.execute("SELECT lookup_key, etag, date_modified FROM idb_object_keys")
    count = 0
    rowcount = 0
    lrc = 0
    for r in cur:
        local_cur.execute("""INSERT INTO media_objects (url,etag,modified) 
        SELECT %(url)s, %(etag)s, %(modified)s WHERE EXISTS (SELECT 1 FROM media WHERE url=%(url)s) AND EXISTS (SELECT 1 FROM objects WHERE etag=%(etag)s) AND NOT EXISTS (SELECT 1 FROM media_objects WHERE url=%(url)s AND etag=%(etag)s)
        """, {"url": r[0], "etag": r[1], "modified": r[2]})
        count += 1
        rowcount += local_cur.rowcount

        if rowcount != lrc and rowcount % 10000 == 0:
            local_pg.commit()
            print count, rowcount
            lrc = rowcount
    local_pg.commit()
    print count, rowcount

def get_objects_from_ceph():
    local_cur.execute("SELECT etag FROM objects")
    existing_objects = set()
    for r in local_cur:
        existing_objects.add(r[0])

    print len(existing_objects)

    s = IDigBioStorage()
    buckets = ["datasets","images"]
    count = 0
    rowcount = 0
    lrc = 0
    for b_k in buckets:
        b = s.get_bucket("idigbio-" + b_k + "-prod")
        for k in b.list():
            if k.name not in existing_objects:
                ks = k.get_contents_as_string(headers={'Range' : 'bytes=0-100'})
                detected_mime = magic.from_buffer(ks, mime=True)
                local_cur.execute("INSERT INTO objects (bucket,etag,detected_mime) SELECT %(bucket)s,%(etag)s,%(dm)s WHERE NOT EXISTS (SELECT 1 FROM objects WHERE etag=%(etag)s)", {"bucket": b_k, "etag": k.name, "dm": detected_mime})
                existing_objects.add(k.name)
                rowcount += local_cur.rowcount
            count += 1


            if rowcount != lrc and rowcount % 10000 == 0:
                print count, rowcount
                local_pg.commit()
                lrc = rowcount
        print count, rowcount
        local_pg.commit()


def get_media_generator():
    local_cur.execute("""SELECT * FROM (
        SELECT substring(url from 'https?://[^/]*/'), count(*) FROM (
            SELECT media.url, media_objects.etag FROM media LEFT JOIN media_objects ON media.url = media_objects.url WHERE type IS NOT NULL AND (last_status IS NULL or (last_status >= 400 and last_check < now() - '1 month'::interval))
        ) AS a WHERE a.etag IS NULL GROUP BY substring(url from 'https?://[^/]*/')
    ) AS b ORDER BY count""")
    subs_rows = local_cur.fetchall()
    for sub_row in subs_rows:
        subs = sub_row[0]
        local_cur.execute("""SELECT url,type,mime FROM (
            SELECT media.url,type,mime,etag FROM media LEFT JOIN media_objects ON media.url = media_objects.url
            WHERE media.url LIKE %s AND type IS NOT NULL AND (last_status IS NULL OR (last_status >= 400 AND last_check < now() - '1 month'::interval))
        ) AS a WHERE a.etag IS NULL""", (subs + "%",))
        url_rows = local_cur.fetchall()
        for url_row in url_rows:
            yield tuple(url_row[0:3])

def get_media_consumer():
    p = Pool(20)
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
            print count,t,f
    print count,t,f

def main():
    import sys
    #create_schema()

    if len(sys.argv) > 1 and sys.argv[1] == "get_media":
        get_media_consumer()
    else:
        media_urls = get_postgres_media_urls()
        write_urls_to_db(media_urls)
        get_objects_from_ceph()
        get_postgres_media_objects()

if __name__ == '__main__':
    main()
