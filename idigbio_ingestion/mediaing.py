from gevent.pool import Pool
from gevent import monkey

monkey.patch_all()

from idb.postgres_backend.db import PostgresDB

import magic
import os
import requests
from requests.auth import HTTPBasicAuth
import traceback

s = requests.Session()

auth = HTTPBasicAuth(os.environ.get("IDB_UUID"), os.environ.get("IDB_APIKEY"))

import sqlite3

sqlite_conn = sqlite3.connect("media.db")
sqlite_conn.isolation_level = None
sqlite_cur = sqlite_conn.cursor()


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


def get_media(tup):
    url, t, fmt = tup

    try:
        media_req = s.get(url)
        media_req.raise_for_status()

        detected_fmt = magic.from_buffer(media_req.content, mime=True)
        if detected_fmt == fmt:
            # apiimg_req = s.post("http://media.idigbio.org/upload/" + t, data={"filereference": url}, files={'file': media_req.content }, auth=auth)
            # apiimg_req.raise_for_status()
            pass
        else:
            print t, detected_fmt, fmt

        return True
    except:
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
        "SELECT COALESCE(data ->> 'ac:accessURI', data ->> 'ac:bestQualityAccessURI', data ->> 'dcterms:identifier') as url, COALESCE(data ->> 'dcterms:format', data ->> 'dc:format') as format FROM data WHERE COALESCE(data ->> 'ac:accessURI', data ->> 'ac:bestQualityAccessURI', data ->> 'dcterms:identifier') IS NOT NULL")
    sqlite_cur.execute("BEGIN")
    for r in cur:
        scanned += 1
        url = r["url"].replace("&amp;", "&").strip()
        if url not in media_urls and url not in inserted_urls:
            if r["format"] in mime_mapping:
                if mime_mapping[r["format"]] is not None:
                    sqlite_cur.execute("INSERT INTO media (url,type,mime) VALUES (?,?,?)", (r[
                                       "url"], mime_mapping[r["format"]], r["format"]))
                    inserts += 1
                    inserted_urls.add(url)
            elif r["format"] is None:
                pass
            else:
                print "Unknown Format", r["format"]
        if inserts >= 10000:
            total_inserts += inserts
            sqlite_conn.commit()
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


def main():
    create_schema()
    media_urls = get_postgres_media_urls()
    write_urls_to_db(media_urls)


if __name__ == '__main__':
    main()
