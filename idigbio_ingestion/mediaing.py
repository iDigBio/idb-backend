from gevent.pool import Pool
from gevent import monkey

monkey.patch_all()

from idb.postgres_backend.db import PostgresDB

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

# Set a mime type to none to explicitly ignore it
mime_mapping = {
    "image/jpeg": "images",
    "text/html": None,
    "image/dng": None,
    "application/xml": None,
}


def create_schema():
    sqlite_cur.execute("BEGIN")
    sqlite_cur.execute("""CREATE TABLE IF NOT EXISTS media (
        url text PRIMARY KEY,
        type text,
        mime text,
        retrieved timestamp,
        etag text,
        detected_mime text
    )
    """)
    sqlite_conn.commit()

ignore_prefix = [
    "http://media.idigbio.org/",
    "http://firuta.huh.harvard.edu/"
]

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

        detected_fmt = magic.from_buffer(media_req.content, mime=True)

        if detected_fmt == fmt:
            #print "Success", url, t, fmt, detected_fmt
            apiimg_req = s.post("http://media.idigbio.org/upload/" + t, data={"filereference": url}, files={'file': media_req.content }, auth=auth)
            apiimg_req.raise_for_status()
            return True
        else:
            if cache_bad:
                with open(url_path,"wb") as outf:
                    outf.write(media_req.content)
            print "Failure", url, t, fmt, detected_fmt
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
    with open("url_out.json_null", "wb") as outf:
        for r in cur:
            scanned += 1

            url = None
            if "ac:accessURI" in r["data"]:
                url = r["data"]["ac:accessURI"]
            elif "ac:bestQualityAccessURI" in r["data"]:
                url = r["data"]["ac:bestQualityAccessURI"]
            elif "dcterms:identifier" in r["data"]:
                url = r["data"]["dcterms:identifier"]
            elif "dc:identifier" in r["data"]:
                url = r["data"]["dc:identifier"]

            form = None
            if "dcterms:format" in r["data"]:
                form = r["data"]["dcterms:format"]
            elif "dc:format" in r["data"]:
                form = r["data"]["dc:format"]

            if url is not None:
                url = url.replace("&amp;", "&").strip()

                for p in ignore_prefix:
                    if url.startswith(p):
                        break
                else:
                    if url not in media_urls and url not in inserted_urls:
                        if form in mime_mapping:
                            if mime_mapping[form] is not None:
                                outf.write(json.dumps([url, mime_mapping[form], form]) + "\x00")
                                inserts += 1
                                inserted_urls.add(url)
                        elif form is None:
                            pass
                        else:
                            print "Unknown Format", form

            if inserts >= 10000:
                total_inserts += inserts
                print total_inserts, scanned
                inserts = 0


def get_postgres_media_urls():
    media_urls = set()

    print "Get Media URLs"

    cur = db._get_ss_cursor()
    cur.execute("SELECT lookup_key FROM idb_object_keys")
    for r in cur:
        media_urls.add(r["lookup_key"])

    return media_urls

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
    p = Pool(50)
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

    if len(sys.argv) > 1 and sys.argv[1] == "get_media":
        get_media_consumer()
    else:
        media_urls = get_postgres_media_urls()
        write_urls_to_db(media_urls)


if __name__ == '__main__':
    main()
