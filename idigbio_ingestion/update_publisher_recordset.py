# must set PYTHONPATH environment variable to the top level prior to running this script
import logging
import re
import datetime
import dateutil.parser
import time
import os

import requests
import feedparser
assert feedparser.__version__ >= "5.2.0"

from idb import config
from idb.postgres_backend.db import PostgresDB, MediaObject, DictCursor
from idb.helpers.etags import calcFileHash
from idb.helpers.storage import IDigBioStorage
from idb.helpers.logging import idblogger

from idigbio_ingestion.lib.util import download_file
from idigbio_ingestion.lib.eml import parseEml

#### disabling warnings per https://urllib3.readthedocs.org/en/latest/security.html#disabling-warnings
## Would rather have warnings go to log but could not get logging.captureWarnings(True) to work.
## There is no urllib3.enable_warnings method. Is it possible to disable_warnings and then re-enable them later?
####### The disable_warnings method did not prevent warnings from being printed. Commenting out for now...
#import urllib3
#assert urllib3.__version__ >= "1.13"
#urllib3.disable_warnings()
####

# uuid '872733a2-67a3-4c54-aa76-862735a5f334' is the idigbio root entity,
# the parent of all publishers.
IDIGBIO_ROOT_UUID = "872733a2-67a3-4c54-aa76-862735a5f334"

logger = idblogger.getChild('upr')

def struct_to_datetime(s):
    """
    Convert a Struct representation of a time to a datetime
    timestamp.

    Parameters
    ----------
    s : struct
        Timestamp in Struct representation, a 9-tuple such as
        (2019, 2, 17, 17, 3, 38, 1, 48, 0)

    Returns
    -------
    datetime timestamp
    """

    return datetime.datetime.fromtimestamp(time.mktime(s))


def id_func(portal_url, e):
    """
    Given a portal url and a feedparser RSS dictionary,
    return something suitable to be used as a recordid.

    Parameters
    ----------
    portal_url : a url to a data portal
    e : feedparser object (feedparser.FeedParserDict)
        An individual rss entry already processed into a feedparser dict.

    """

    id = None
    if "id" in e:   # feedparser magic maps various fields to "id"
        id = e["id"]
    elif "collid" in e:
        id = "{0}collections/misc/collprofiles.php?collid={1}".format(
            portal_url, e["collid"])

    if id is not None:
        # Strip version from ipt ids
        m = re.search('^(.*)/v[0-9]*(\.)?[0-9]*$', id)
        if m is not None:
            id = m.group(1)

        id = id.lower()
    return id


def get_feed(rss_url):
    """
    Download contents of an RSS feed into a string.
    This is required since feedparser itself doesn't have a timeout parameter

    Parameters
    ----------
    rss_url : str
        The URI of an rss feed

    Returns
    -------
    text or False
        Content of the RSS feed (body / text) if we are able to download it
        from the URI, otherwise return False.

    """

    feedtest = None
    try:
        feedtest = requests.get(rss_url, timeout=10)
        feedtest.raise_for_status()
    except requests.exceptions.SSLError:
        # Ignore urllib3 SSL issues on this check?
        # Most of the time, SSL issues are simply certificate errors at the provider side and we feel ok skipping.
        #
        # However, "special" kinds of server errors such as documented in redmine #2114 get skipped if we do nothing, hence the extra check below.
        pass
    except Exception as e:
        logger.error("Failed to read %r; reason: %s",
                     rss_url,
                     feedtest.reason if feedtest is not None else "non-http error")
        if feedtest is None:
            logger.error("Specific reason: %s", e)
        return False
    # At this point we could have feedtest = None coming out of the SSLError exception above.
    if feedtest is None:
        logger.error("Feed error on rss_url = %r", rss_url)
        return False
    else:
        return feedtest.text


def update_db_from_rss():
    # existing_recordsets is a dict that will hold mapping of recordids to db id
    existing_recordsets = {}
    # recordsets is a dict that will hold rows based on db id (not recordid or uuid)
    recordsets = {}
    with PostgresDB() as db:
        logger.debug("Gathering existing recordsets...")
        for row in db.fetchall("SELECT * FROM recordsets"):
            for recordid in row["recordids"]:
                logger.debug("id | recordid | file_link: '{0}' | '{1}' | '{2}'".format(
                    row["id"], recordid, row["file_link"]))
                if recordid in existing_recordsets:
                    logger.error("Found a duplicate recordid in existing recordsets. This should never happen.")
                    logger.error("This recordid = '{0}'"
                        ". Other row = ''{1}''".format(recordid, existing_recordsets[recordid]))
                else:
                    existing_recordsets[recordid] = row["id"]
            recordsets[row["id"]] = row

        logger.debug("Gathering existing publishers...")
        pub_recs = db.fetchall("SELECT * FROM publishers")
        logger.debug("Checking %d publishers", len(pub_recs))
        for row in pub_recs:
            uuid, rss_url = row['uuid'], row['rss_url']
            logger.info("Starting Publisher Feed: %s %s", uuid, rss_url)
            rsscontents = get_feed(rss_url)
            if rsscontents:
                try:
                    _do_rss(rsscontents, row, db, recordsets, existing_recordsets)
                    db.commit()
                except Exception:
                    logger.exception("Error with %s %s", uuid, rss_url)
                    db.rollback()
                except:
                    db.rollback()
                    raise
    logger.info("Finished processing add publisher RSS feeds")


def _do_rss(rsscontents, r, db, recordsets, existing_recordsets):
    """
    Process one RSS feed contents.  Compares the recordsets we know
    about with the ones found in the feed.

    Parameters
    ----------
    rsscontents : text
        Content of an RSS feed
    r : row of publisher data
        A row of data from the publishers table that contains all columns,
        each column addressable as r["column_name"]
    db : database object
        A PostgresDB() database object
    recordsets : set
        dict of existing known recordset db ids with associated db row data
    existing_recordsets : dict
        dict of existing known recordset recordids with associated db ids
    """

    logger.debug("Start parsing results of %s, length: %s", r['rss_url'], len(rsscontents))
    feed = feedparser.parse(rsscontents)
    pub_uuid = r["uuid"]
    if pub_uuid is None:
        pub_uuid, _, _ = db.get_uuid(r["recordids"])

    name = r["name"]
    if name is None or name == "":
        if "title" in feed["feed"]:
            name = feed["feed"]["title"]
            if name == "":
                name = r["rss_url"]
        else:
            name = r["rss_url"]

    if "\\x" in name:
        name = name.decode("utf8")

    logger.info("Update Publisher id:%s %s '%s'", r["id"], pub_uuid, name)

    auto_publish = r["auto_publish"]

    pub_date = None
    if "published_parsed" in feed["feed"]:
        pub_date = struct_to_datetime(feed["feed"]["published_parsed"])
    elif "updated_parsed" in feed:
        pub_date = struct_to_datetime(feed["updated_parsed"])
    elif "updated_parsed" in feed["feed"]:
        pub_date = struct_to_datetime(feed["feed"]["updated_parsed"])
    sql = ("""UPDATE publishers
              SET name=%(name)s,
                  last_seen=%(last_seen)s,
                  pub_date=%(pub_date)s,
                  uuid=%(uuid)s
              WHERE id=%(id)s""",
           {
               "id": r["id"],
               "name": name,
               "uuid": pub_uuid,
               "last_seen": datetime.datetime.now(),
               "pub_date": pub_date,
           })
    db.execute(*sql)

    logger.debug("Begin iteration over entries found in '{0}'".format(r['rss_url']))
    for e in feed['entries']:
        recordid = id_func(r['portal_url'], e)
        logger.debug ("id_func returned '{0}'".format(recordid))
        rsid = None
        ingest = auto_publish
        recordids = [recordid]
        recordset = None
        if recordid in existing_recordsets:
            logger.debug("Found recordid '{0}' in existing recordsets.".format(recordid))
            recordset = recordsets[existing_recordsets[recordid]]
            logger.debug("recordset = '{0}'".format(recordset))
            rsid = recordset["uuid"]
            ingest = recordset["ingest"]
            recordids = list(set(recordids + recordset["recordids"])) # is this set dropping important info
        else:
            logger.debug("recordid '{0}' NOT found in existing recordsets.".format(recordid))

        eml_link = None
        file_link = None
        date = None
        rs_name = None

        if "published_parsed" in e and e["published_parsed"] is not None:
            date = struct_to_datetime(e["published_parsed"])
            logger.debug('pub_date struct via published_parsed: {0}'.format(date.isoformat()))
        elif "published" in e and e["published"] is not None:
            date = dateutil.parser.parse(e["published"])
            logger.debug('pub_date via dateutil: {0}'.format(date.isoformat()))

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

        if recordid is not None:
            logger.debug("Identified recordid:  '{0}'".format(recordid))
        else:
            logger.debug("No recordid identified.")



        if recordset is None:
            sql = (
                """INSERT INTO recordsets
                     (uuid, publisher_uuid, name, recordids, eml_link, file_link, ingest, pub_date)
                   VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
                   ON CONFLICT (file_link) DO UPDATE set recordids=array_append(recordsets.recordids,%s), pub_date=%s,
                   last_seen = now()
                """,
                (rsid, pub_uuid, rs_name, recordids, eml_link, file_link, ingest, date, recordid, date))
            db.execute(*sql)
            logger.info("Create Recordset for recordid:%s '%s'", recordid, rs_name)
        else:
            sql = ("""UPDATE recordsets
                      SET publisher_uuid=%(publisher_uuid)s,
                          eml_link=%(eml_link)s,
                          file_link=%(file_link)s,
                          last_seen=%(last_seen)s,
                          pub_date=%(pub_date)s
                      WHERE id=%(id)s""",
                   {
                       "publisher_uuid": pub_uuid,
                       "name": rs_name,
                       "recordids": recordids,
                       "eml_link": eml_link,
                       "file_link": file_link,
                       "last_seen": datetime.datetime.now(),
                       "pub_date": date,
                       "id": recordset["id"]
                   })
            db.execute(*sql)
            logger.info("Update Recordset id:%s %s %s '%s'",
                        recordset["id"], recordset["uuid"], file_link, rs_name)


    db.set_record(pub_uuid, "publisher", IDIGBIO_ROOT_UUID,
                  {
                      "rss_url": r["rss_url"],
                      "name": name,
                      "auto_publish": r["auto_publish"],
                      "base_url": r["portal_url"],
                      "publisher_type": r["pub_type"],
                      "recordsets": {}
                  },
                  r["recordids"], [])


def harvest_all_eml():
    sql = """SELECT *
             FROM recordsets
             WHERE eml_link IS NOT NULL
               AND ingest=true
               AND pub_date < now()
               AND (eml_harvest_date IS NULL OR eml_harvest_date < pub_date)"""
    with PostgresDB() as db:
        recs = db.fetchall(sql, cursor_factory=DictCursor)
        logger.info("Harvesting %d EML files", len(recs))
        for r in recs:
            try:
                harvest_eml(r, db)
                db.commit()
            except KeyboardInterrupt:
                db.rollback()
                raise
            except:
                db.rollback()
                logger.exception("failed Harvest EML %s %s", r["id"], r["name"])

def harvest_eml(r, db):
    logger.info("Harvest EML %s '%s' @ '%s'", r["id"], r["name"], r["eml_link"])
    fname = "{0}.eml".format(r["id"])
    if not download_file(r["eml_link"], fname):
        logger.error("failed Harvest EML %s '%s' @ '%s'", r["id"], r["name"], r["eml_link"])
        return
    try:
        etag = calcFileHash(fname)
        u = r["uuid"]
        if u is None:
            logger.debug("No uuid, using get_uuid on recordids")
            u, _, _ = db.get_uuid(r["recordids"])
        logger.debug("Using recordset UUID: {0}".format(u))
        desc = {}
        with open(fname,"rb") as inf:
            desc = parseEml(r["recordids"][0], inf.read())
        desc["ingest"] = r["ingest"]
        desc["link"] = r["file_link"]
        desc["eml_link"] = r["eml_link"]
        desc["update"] = r["pub_date"].isoformat()
        parent = r["publisher_uuid"]
        db.set_record(u, "recordset", parent, desc, r["recordids"], [])
        sql = ("""UPDATE recordsets
                  SET eml_harvest_etag=%s, eml_harvest_date=%s, uuid=%s
                  WHERE id=%s""",
               (etag, datetime.datetime.now(), u, r["id"]))
        db.execute(*sql)
    finally:
        if os.path.exists(fname):
            os.unlink(fname)


def harvest_all_file():
    sql = """SELECT *
             FROM recordsets
             WHERE file_link IS NOT NULL
               AND uuid IS NOT NULL
               AND ingest=true
               AND pub_date < now()
               AND (file_harvest_date IS NULL OR file_harvest_date < pub_date)"""

    with PostgresDB() as db:
        recs = db.fetchall(sql)
        logger.info("Harvesting %d files", len(recs))
        for r in recs:
            try:
                harvest_file(r, db)
                db.commit()
            except KeyboardInterrupt:
                db.rollback()
                raise
            except:
                logger.exception("Error processing id:%s url:%s", r['id'], r['file_link'])
                db.rollback()

def harvest_file(r, db):
    logger.info("Harvest File %s '%s' @ '%s'", r["id"], r["name"], r["file_link"])
    fname = "{0}.file".format(r["id"])

    if not download_file(r["file_link"], fname, timeout=5):
        logger.error("failed Harvest file %s '%s' @ '%s'", r["id"], r["name"], r["file_link"])
        return
    try:
        etag = upload_recordset(r["uuid"], fname, db)
        assert etag
        sql = ("""UPDATE recordsets
                  SET file_harvest_etag=%s, file_harvest_date=%s
                  WHERE id=%s""",
               (etag, datetime.datetime.now(), r["id"]))
        db.execute(*sql)
    finally:
        if os.path.exists(fname):
            os.unlink(fname)


def upload_recordset(rsid, fname, idbmodel):
    filereference = "http://api.idigbio.org/v1/recordsets/" + rsid
    logger.debug("Starting Upload of %r", rsid)
    stor = IDigBioStorage()
    with open(fname, 'rb') as fobj:
        mo = MediaObject.fromobj(
            fobj, url=filereference, type='datasets', owner=config.IDB_UUID)
        k = mo.get_key(stor)
        if k.exists():
            logger.debug("ETAG %s already present in Storage.", mo.etag)
        else:
            mo.upload(stor, fobj)
            logger.debug("ETAG %s uploading from %r", mo.etag, fname)

        mo.ensure_media(idbmodel)
        mo.ensure_object(idbmodel)
        mo.ensure_media_object(idbmodel)
        return mo.etag
    logger.debug("Finished Upload of %r", rsid)


def create_tables():
    """
    This function is out-of-sync with actual database, unmaintained.
    Commenting out all action in this function, it will do nothing until modified again.
    """

    db = PostgresDB()
    logger.error('create_tables called but has no valid code to run.')

    # db.execute("""CREATE TABLE IF NOT EXISTS publishers (
    #     id BIGSERIAL NOT NULL PRIMARY KEY,
    #     uuid uuid UNIQUE,
    #     name text NOT NULL,
    #     recordids text[] NOT NULL DEFAULT '{}',
    #     pub_type varchar(20) NOT NULL DEFAULT 'rss',
    #     portal_url text,
    #     rss_url text NOT NULL,
    #     auto_publish boolean NOT NULL DEFAULT false,
    #     first_seen timestamp NOT NULL DEFAULT now(),
    #     last_seen timestamp NOT NULL DEFAULT now(),
    #     pub_date timestamp
    # )""")

    # #pubid, rsid  Ingest, rs_record_id, eml_link, file_link, First Seen Date, Last Seen Date, Feed Date, Harvest Date, Harvest Etag
    # db.execute("""CREATE TABLE IF NOT EXISTS recordsets (
    #     id BIGSERIAL NOT NULL PRIMARY KEY,
    #     uuid uuid UNIQUE,
    #     publisher_uuid uuid REFERENCES publishers(uuid),
    #     name text NOT NULL,
    #     recordids text[] NOT NULL DEFAULT '{}',
    #     eml_link text,
    #     file_link text NOT NULL,
    #     ingest boolean NOT NULL DEFAULT false,
    #     first_seen timestamp NOT NULL DEFAULT now(),
    #     last_seen timestamp NOT NULL DEFAULT now(),
    #     pub_date timestamp,
    #     harvest_date timestamp,
    #     harvest_etag varchar(41)
    # )""")
    # db.commit()
    db.close()


def main():
    # create_tables()
    # Re-work from canonical db
    logger.info("Begin update_publisher_recordset()")
    update_db_from_rss()
    logger.info("*** Begin harvest of eml files...")
    harvest_all_eml()
    logger.info("*** Begin harvest of dataset files...")
    harvest_all_file()
    logger.info("Finished all updates")
