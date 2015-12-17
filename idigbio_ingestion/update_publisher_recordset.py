import feedparser
assert feedparser.__version__ >= "5.2.0"
import re
import datetime
import dateutil.parser
import time
import requests
from requests.auth import HTTPBasicAuth
import traceback
import uuid
import os

# must set PYTHONPATH environment variable to the top level prior to running this script

from idb.postgres_backend.db import PostgresDB

from lib.util import download_file
from idb.helpers.etags import calcFileHash
from lib.eml import parseEml
from lib.log import logger

#### disabling warnings per https://urllib3.readthedocs.org/en/latest/security.html#disabling-warnings
## Would rather have warnings go to log but could not get logging.captureWarnings(True) to work.
## There is no urllib3.enable_warnings method. Is it possible to disable_warnings and then re-enable them later?
####### The disable_warnings method did not prevent warnings from being printed. Commenting out for now...
#import urllib3
#assert urllib3.__version__ >= "1.13"
#urllib3.disable_warnings()
####


def struct_to_datetime(s):
    return datetime.datetime.fromtimestamp(time.mktime(s))

db = PostgresDB()

def create_tables():
    db._cur.execute("""CREATE TABLE IF NOT EXISTS publishers (
        id BIGSERIAL NOT NULL PRIMARY KEY,
        uuid uuid UNIQUE,
        name text NOT NULL,
        recordids text[] NOT NULL DEFAULT '{}',
        pub_type varchar(20) NOT NULL DEFAULT 'rss',
        portal_url text,
        rss_url text NOT NULL,
        auto_publish boolean NOT NULL DEFAULT false,
        first_seen timestamp NOT NULL DEFAULT now(),
        last_seen timestamp NOT NULL DEFAULT now(), 
        pub_date timestamp
    )""")

    #pubid, rsid  Ingest, rs_record_id, eml_link, file_link, First Seen Date, Last Seen Date, Feed Date, Harvest Date, Harvest Etag
    db._cur.execute("""CREATE TABLE IF NOT EXISTS recordsets (
        id BIGSERIAL NOT NULL PRIMARY KEY,
        uuid uuid UNIQUE,
        publisher_uuid uuid REFERENCES publishers(uuid),
        name text NOT NULL,
        recordids text[] NOT NULL DEFAULT '{}',
        eml_link text,
        file_link text NOT NULL,
        ingest boolean NOT NULL DEFAULT false,
        first_seen timestamp NOT NULL DEFAULT now(),
        last_seen timestamp NOT NULL DEFAULT now(),
        pub_date timestamp,
        harvest_date timestamp,
        harvest_etag varchar(41)
    )""")

    db.commit()

def id_func(e):
    id = None
    if "id" in e:   # feedparser magic maps various fields to "id"
        id = e["id"]
    elif "collid" in e:
        id = "{0}collections/misc/collprofiles.php?collid={1}".format(
            self.portal_url, e["collid"])

    if id is not None:
        # Strip version from ipt ids
        m = re.search('^(.*)/v[0-9]*(\.)?[0-9]*$', id)
        if m is not None:
            id = m.group(1)

        id = id.lower()
    return id

def update_db_from_rss():
    existing_recordsets = {}
    recordsets = {}

    db._cur.execute("SELECT * FROM recordsets")
    for r in db._cur:
        for recordid in r["recordids"]:
            existing_recordsets[recordid] = r["id"]
        recordsets[r["id"]] = r

    db._cur.execute("SELECT * FROM publishers")
    pub_recs = db._cur.fetchall()
    for r in pub_recs:
        feedisgood = True
        logger.info("Publisher Feed: {0} {1}".format(r["uuid"], r["rss_url"]))
        # Quick check on the feed url since feedparser does not have a timeout parameter
        try:
            feedtest = requests.get(r["rss_url"],timeout=10)
            feedtest.raise_for_status()
        except requests.exceptions.SSLError:
            # Ignore urllib3 SSL issues on this quick check
            pass
        except:
            feedisgood = False
            logger.error("Failed to read {0}".format(r["rss_url"]))
        
        if feedisgood:
            try:

                feed = feedparser.parse(r["rss_url"])

                pub_uuid = r["uuid"]
                if pub_uuid is None:
                    pub_uuid, _, _ = db.get_uuid(r["recordids"])

                name = r["name"]
                if name is None:
                    if "title" in feed["feed"]:
                        name = feed["feed"]["title"]
                    else:
                        name = r["rss_url"]

                if "\\x" in name:
                    name = name.decode("utf8")

                auto_publish = r["auto_publish"]

                logger.info("Update Publisher id:"+str(r["id"]) + " " + pub_uuid + " " + name )

                pub_date = None
                if "published_parsed" in feed["feed"]:
                    pub_date = struct_to_datetime(feed["feed"]["published_parsed"])
                elif "updated_parsed" in feed:
                    pub_date = struct_to_datetime(feed["updated_parsed"])
                elif "updated_parsed" in feed["feed"]:
                    pub_date = struct_to_datetime(feed["feed"]["updated_parsed"])

                db._cur.execute("""UPDATE publishers SET
                        name=%(name)s,
                        last_seen=%(last_seen)s,
                        pub_date=%(pub_date)s,
                        uuid=%(uuid)s
                        WHERE id=%(id)s
                    """,
                    {
                        "id": r["id"],
                        "name": name,
                        "uuid": pub_uuid,
                        "last_seen": datetime.datetime.now(),
                        "pub_date": pub_date,
                    }
                )

                for e in feed['entries']:
                    recordid = id_func(e)

                    rsid = None
                    ingest = auto_publish
                    recordids = [recordid]
                    recordset = None           
                    if recordid in existing_recordsets:
                        recordset = recordsets[existing_recordsets[recordid]]
                        rsid = recordset["uuid"]
                        ingest = recordset["ingest"]
                        recordids = list(set(recordids + recordset["recordids"]))

                    eml_link = None
                    file_link = None
                    date = None
                    rs_name = None

                    if "published_parsed" in e and e["published_parsed"] is not None:
                        date = struct_to_datetime(e["published_parsed"])
                    elif "published" in e and e["published"] is not None:
                        date = dateutil.parser.parse(e["published"])

                    # Pick a time distinctly before now() to avoid data races
                    fifteen_minutes_ago = datetime.datetime.now() - datetime.timedelta(minutes=15)
                    if date is None or date > datetime.datetime.now():
                        date = fifteen_minutes_ago

                    for eml_prop in ["ipt_eml", "emllink"]:
                        if eml_prop in e:
                            eml_link = e[eml_prop]
                            break
                    else:
                        if recordset is not None:
                            eml_link = recordset["eml_link"]

                    for link_prop in ["ipt_dwca", "link"]:
                        if link_prop in e:
                            file_link = e[link_prop]
                            break
                    else:
                        if recordset is not None:
                            file_link = recordset["file_link"]

                    if "title" in e:
                        rs_name = e['title']
                    elif recordset is not None:
                        rs_name = recordset["name"]
                    else:
                        rs_name = recordid

                    if recordset is None:
                        db._cur.execute(             
                            """INSERT INTO recordsets 
                                (uuid, publisher_uuid, name, recordids, eml_link, file_link, ingest, pub_date)
                                VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
                            """,
                            (rsid, pub_uuid, rs_name, recordids, eml_link, file_link, ingest, date)
                        )
                        logger.info("Create Recordset " + recordid + " " + name)
                    else:
                        db._cur.execute("""UPDATE recordsets SET
                                publisher_uuid=%(publisher_uuid)s,
                                eml_link=%(eml_link)s,
                                file_link=%(file_link)s,
                                last_seen=%(last_seen)s,
                                pub_date=%(pub_date)s
                                WHERE id=%(id)s
                            """,
                            {
                                "publisher_uuid": pub_uuid,
                                "name": rs_name,
                                "recordids": recordids,
                                "eml_link": eml_link,
                                "file_link": file_link,
                                "last_seen": datetime.datetime.now(),
                                "pub_date": date,
                                "id": recordset["id"]
                            }
                        )
                        logger.info("Update Recordset id:" + str(recordset["id"]) + " " + recordid + " " + name)


                db.set_record(pub_uuid,"publisher","872733a2-67a3-4c54-aa76-862735a5f334",{
                    "rss_url": r["rss_url"],
                    "name": name,
                    "auto_publish": r["auto_publish"],
                    "base_url": r["portal_url"],
                    "publisher_type": r["pub_type"],
                    "recordsets": {}
                },r["recordids"],[],commit=False)
                db.commit()
            except:
                print r
                traceback.print_exc()
                db.rollback()

def harvest_eml():
    s = requests.Session()
    db._cur.execute("SELECT * FROM recordsets WHERE eml_link IS NOT NULL AND ingest=true AND pub_date < now() AND (eml_harvest_date IS NULL OR eml_harvest_date < pub_date)")
    recs = db._cur.fetchall()
    for r in recs:
        logger.info("Harvest EML " + str(r["id"]) + " " + r["name"])
        fname = "{0}.eml".format(r["id"])
        if not download_file(r["eml_link"],fname):
            logger.error("failed Harvest EML " + str(r["id"]) + " " + r["name"])
        else:
            try:
                etag = calcFileHash(fname)
                u = r["uuid"]
                #logger.debug("u = " + u)
                if u is None:
                    u, _, _ = db.get_uuid(r["recordids"])
                desc = {}
                with open(fname,"rb") as inf:
                    desc = parseEml(r["recordids"][0], inf.read())
                desc["ingest"] = r["ingest"]
                desc["link"] = r["file_link"]
                desc["eml_link"] = r["eml_link"]
                desc["update"] = r["pub_date"].isoformat()
                parent = r["publisher_uuid"]
                db.set_record(u,"recordset",parent,desc,r["recordids"],[],commit=False)
                db._cur.execute("UPDATE recordsets SET eml_harvest_etag=%s, eml_harvest_date=%s,uuid=%s WHERE id=%s", (etag,datetime.datetime.now(),u,r["id"]))
                db.commit()
            except:
                logger.error("failed Harvest EML " + str(r["id"]) + " " + r["name"])
                traceback.print_exc()
        if os.path.exists(fname):
            os.unlink(fname)

def upload_recordset_to_mediaapi(rsid, fname):
    try:
        with open(fname,'rb') as inf:
            files = {'file': inf}
            r = requests.post("http://media.idigbio.org/upload/datasets", files=files, data={"filereference": "http://api.idigbio.org/v1/recordsets/"+rsid}, auth=auth)
            r.raise_for_status()
        return True
    except KeyboardInterrupt:
        raise
    except Exception,e:
        logger.error("failed to post recordset " + rsid)
        traceback.print_exc()
        return False

auth=HTTPBasicAuth(os.environ["IDB_UUID"],os.environ["IDB_APIKEY"])
def harvest_file():
    s = requests.Session()
    db._cur.execute("SELECT * FROM recordsets WHERE file_link IS NOT NULL AND uuid IS NOT NULL AND ingest=true AND pub_date < now() AND (file_harvest_date IS NULL OR file_harvest_date < pub_date)")
    recs = db._cur.fetchall()
    for r in recs:
        logger.info("Harvest File " + str(r["id"]) + " " + r["name"])
        fname = "{0}.file".format(r["id"])
        try:
            download_file(r["file_link"],fname)
            etag = calcFileHash(fname)
            if etag != r["file_harvest_etag"]:
                upload_recordset_to_mediaapi(r["uuid"], fname)
            db._cur.execute("UPDATE recordsets SET file_harvest_etag=%s, file_harvest_date=%s WHERE id=%s", (etag,datetime.datetime.now(),r["id"]))
            db.commit()
        except:
            traceback.print_exc()   
        if os.path.exists(fname):
            os.unlink(fname)

def main():
    # create_tables()
    # Re-work from canonical db
    update_db_from_rss()
    harvest_eml()
    harvest_file()

if __name__ == '__main__':
    main()
