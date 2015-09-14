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
        mime varchar(255)
    )
    """)
    local_cur.execute("""CREATE TABLE IF NOT EXISTS objects (
        id BIGSERIAL PRIMARY KEY,
        bucket varchar(255) NOT NULL,
        etag varchar(41) NOT NULL UNIQUE,
        detected_mime varchar(255)
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
    "model/mesh": lambda url, t, fmt: url.endswith(".stl")
}

def default_format_validator(url, t, fmt):
    return fmt == magic.from_buffer(media_req.content, mime=True)

def get_media(tup, cache_bad=False):
    url, t, fmt = tup

    url_path = "bad_media/"+url.replace("/","^^")

    try:
        for p in ignore_prefix:
            if url.startswith(p):
                print "Skip", url, t, fmt, p
                return False

        media_req = s.get(url)
        media_req.raise_for_status()

        if fmt in format_validators:
            validator = format_validators[fmt]
        else:
            validator = default_format_validator

        if validator(url,t,fmt):
            #print "Success", url, t, fmt, detected_fmt
            apiimg_req = s.post("http://media.idigbio.org/upload/" + t, data={"filereference": url}, files={'file': media_req.content }, auth=auth)
            apiimg_req.raise_for_status()
            return True
        else:
            if cache_bad:
                with open(url_path,"wb") as outf:
                    outf.write(media_req.content)
            print "Failure", url, t, fmt
            return False
    except KeyboardInterrupt as e:
        raise e
    except:
        print url, t, fmt
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

    cur = db._get_ss_cursor()
    cur.execute("SELECT url FROM media")
    for r in cur:
        media_urls.add(r[0])

    return media_urls

def get_postgres_media_objects():
    cur = db._get_ss_cursor()
    cur.execute("SELECT lookup_key, etag, date_modified FROM idb_object_keys")
    count = 0
    for r in cur:
        local_cur.execute("""INSERT INTO media_objects (url,etag,modified) 
        SELECT %(url)s, %(etag)s, %(modified)s WHERE EXISTS (SELECT 1 FROM media WHERE url=%(url)s)
        """, {"url": r[0], "etag": r[1], "modified": r[2]})
        count += 1

        if count % 10000 == 0:
            local_pg.commit()
            print count
    local_pg.commit()
    print count

def get_objects_from_ceph():
    local_cur.execute("SELECT etag FROM objects")
    existing_objects = set()
    for r in local_cur:
        existing_objects.add(r[0])

    print len(existing_objects)

    s = IDigBioStorage()
    buckets = ["datasets","images"]
    count = 0
    for b_k in buckets:
        b = s.get_bucket("idigbio-" + b_k + "-prod")
        for k in b.list():
            if k.name not in existing_objects:
                ks = k.get_contents_as_string(headers={'Range' : 'bytes=0-100'})
                detected_mime = magic.from_buffer(ks, mime=True)
                local_cur.execute("INSERT INTO objects (bucket,etag,detected_mime) VALUES (%s,%s,%s)", (b_k,k.name,detected_mime))
                existing_objects.add(k.name)
            count += 1

            if count % 10000 == 0:
                print count
                local_pg.commit()
        print count
        local_pg.commit()


def get_media_generator():
    with open("url_out.json_null", "rb") as inf:
        r = inf.read(4096)
        end = ""
        while r is not None:
            lines = (end + r).split("\x00")
            end = lines[-1]
            for l in lines[:-1]:
                yield tuple(json.loads(l))
            r = inf.read(4096)

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

def main():
    import sys
    create_schema()
    #get_objects_from_ceph()
    get_postgres_media_objects()

    # if len(sys.argv) > 1 and sys.argv[1] == "get_media":
    #     get_media_consumer()
    # else:
    #     media_urls = get_postgres_media_urls()
    #     write_urls_to_db(media_urls)


if __name__ == '__main__':
    main()
