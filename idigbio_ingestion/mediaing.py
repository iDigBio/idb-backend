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

auth = HTTPBasicAuth(os.environ.get("IDB_UUID"),os.environ.get("IDB_APIKEY"))

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

def main():
    db = PostgresDB()

    media_urls = set()

    print "Get Media URLs"

    cur = db._get_ss_cursor()
    cur.execute("SELECT lookup_key FROM idb_object_keys")
    for r in cur:
        media_urls.add(r["lookup_key"])

    # Set a mime type to none to explicitly ignore it
    mime_mapping = {
        "image/jpeg": "images",
        "text/html": None,
        "image/dng": None,
        "application/xml": None,
    }

    def media_url_iterator():
        print "Start Iterator"
        cur = db._get_ss_cursor()
        cur.execute("SELECT COALESCE(data ->> 'ac:accessURI', data ->> 'ac:bestQualityAccessURI', data ->> 'dcterms:identifier') as url, COALESCE(data ->> 'dcterms:format', data ->> 'dc:format') as format FROM data WHERE COALESCE(data ->> 'ac:accessURI', data ->> 'ac:bestQualityAccessURI', data ->> 'dcterms:identifier') IS NOT NULL")
        for r in cur:
            url = r["url"].replace("&amp;","&").strip()
            if url not in media_urls:
                if r["format"] in mime_mapping:
                    if mime_mapping[r["format"]] is not None:
                        yield (r["url"], mime_mapping[r["format"]], r["format"])
                elif r["format"] is None:
                    pass
                else:
                    print "Unknown Format", r["format"]

    p = Pool()
    for _ in p.imap(get_media,media_url_iterator()):
        pass

if __name__ == '__main__':
    main()