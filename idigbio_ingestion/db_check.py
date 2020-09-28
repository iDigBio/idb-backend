from __future__ import absolute_import, print_function
import datetime
import functools
import json
import logging
import os
import re
import traceback

import magic

from atomicfile import AtomicFile
from psycopg2 import DatabaseError
from psycopg2.extras import DictCursor

from boto.exception import S3ResponseError, S3DataError


from idb import stats
from idb import config
from idb.postgres_backend import apidbpool, NamedTupleCursor
from idb.postgres_backend.db import PostgresDB, RecordSet
from idb.helpers import ilen
from idb.helpers.etags import calcEtag, calcFileHash
from idb.helpers.logging import idblogger, LoggingContext
from idb.helpers.storage import IDigBioStorage
from idb.helpers import gipcpool

from idigbio_ingestion.lib.dwca import Dwca
from idigbio_ingestion.lib.delimited import DelimitedFile


bad_chars = u"\ufeff"
bad_char_re = re.compile("[%s]" % re.escape(bad_chars))

logger = idblogger.getChild("db-check")

uuid_re = re.compile(
    "([a-fA-F0-9]{8}-[a-fA-F0-9]{4}-[a-fA-F0-9]{4}-[a-fA-F0-9]{4}-[a-fA-F0-9]{12})")

s = IDigBioStorage()

class RecordException(Exception):
    pass

def getrslogger(rsid):
    return logger.getChild(rsid)

def mungeid(s):
    return bad_char_re.sub('', s).strip()

identifier_fields = {
    "dwc:Occurrence": [
        ("idigbio:recordId", lambda r, rs: mungeid(r)),
        ("idigbio:recordID", lambda r, rs: mungeid(r)),
        ("dwc:ResourceRelationship", lambda r, rs: idFromRR(r, rs=rs)),
        ("dwc:occurrenceID", lambda r, rs: rs + "\\" + mungeid(r)),
        ("id", lambda r, rs: rs + "\\" + mungeid(r)),
        ("ID", lambda r, rs: rs + "\\" + mungeid(r)),
    ],
    "dwc:Multimedia": [
        ("idigbio:recordId", lambda r, rs: mungeid(r)),
        ("idigbio:recordID", lambda r, rs: mungeid(r)),
        ("ac:providerManagedID", lambda r, rs: mungeid(r)),
        ("dcterms:identifier", lambda r, rs: rs + "\\media\\" + mungeid(r)),
        # ("coreid", lambda r,rs: rs + "\\" + r)
    ],
    "dcterms": [
        ("idigbio:recordId", lambda r, rs: mungeid(r)),
        ("idigbio:recordID", lambda r, rs: mungeid(r)),
        ("ac:providerManagedID", lambda r, rs: mungeid(r)),
        ("dcterms:identifier", lambda r, rs: rs + "\\media\\" + mungeid(r)),
        # ("coreid", lambda r,rs: rs + "\\" + r)
    ]
}

ingestion_types = {
    "dwc:Occurrence": "records",
    "dwc:Multimedia": "mediarecords",
    "dcterms": "mediarecords",
    "records": "records",
    "mediarecords": "mediarecords",
}


def idFromRR(r, rs=None):
    for idr in r:
        if "dwc:relatedResourceID" in idr and "dwc:relationshipOfResource" in idr:
            if idr["dwc:relationshipOfResource"] == "representedIn":
                return mungeid(idr["dwc:relatedResourceID"])
            elif rs is not None and idr["dwc:relationshipOfResource"] == "sameAs":
                return rs + "\\" + mungeid(idr["dwc:relatedResourceID"])
            else:
                return None
        else:
            return None


def get_file(rsid):
    fname = rsid
    if not os.path.exists(fname):
        try:
            RecordSet.fetch_file(rsid, fname, media_store=IDigBioStorage(), logger=logger.getChild(rsid))
        except (S3ResponseError, S3DataError):
            getrslogger(rsid).exception("failed fetching archive")
            raise
    mime = magic.from_file(fname, mime=True)
    return (fname, mime)


def get_db_dicts(rsid):
    id_uuid = {}
    uuid_etag = {}
    for t in ["record", "mediarecord"]:
        id_uuid[t + "s"] = {}
        uuid_etag[t + "s"] = {}
        for c in PostgresDB().get_children_list_for_ingest(rsid, t, cursor_factory=NamedTupleCursor):
            u = c.uuid
            e = c.etag
            uuid_etag[t + "s"][u] = e
            for i in c.recordids:
                id_uuid[t + "s"][i] = u
    return (uuid_etag, id_uuid)


def identifyRecord(t, etag, r, rsid):
    idents = []
    if t in identifier_fields:
        for pi in identifier_fields[t]:
            if pi[0] in r:
                # The "UConn exception"
                # (After working initially, it now appears to allow numeric integers into uuids_identifier,
                # screwing up morphosource et al...)
                if pi[0] == "ac:providerManagedID" and "dcterms:identifier" in r and r["dcterms:identifier"].lower() == r["ac:providerManagedID"].lower():
                    continue
                cid = pi[1](r[pi[0]], rsid)
                if cid is not None:
                    idents.append((etag, pi[0], cid.lower()))
    return idents

unconsumed_extensions = {}
core_siblings = {}


def process_subfile(rf, rsid, rs_uuid_etag, rs_id_uuid, ingest=False, db=None):
    """
    Processes a data file (typically one of multiple files inside a DwCA).

    rf : idigbio_ingestion.lib.dwca.DwcaRecordFile

    Returns
    -------
    Dict
        A data structure containing summary information on the results of the subfile processing.
    """
    rlogger = getrslogger(rsid)

    count = 0
    no_recordid_count = 0
    duplicate_record_count = 0
    duplicate_id_count = 0

    seen_etags = set()
    seen_ids = {}
    seen_uuids = {}

    dupe_ids = set()
    dupe_etags = set()

    found = 0
    match = 0
    ingestions = 0
    assertions = 0
    to_undelete = 0
    resurrections = 0
    record_exceptions = 0
    exceptions = 0
    dberrors = 0

    typ = None

    if rf.rowtype in ingestion_types:
        typ = ingestion_types[rf.rowtype]
        existing_etags = rs_uuid_etag[typ]
        existing_ids = rs_id_uuid[typ]
    else:
        existing_etags = {}
        existing_ids = {}
        ingest = False

    t = datetime.datetime.now()
    rlogger.info("Beginning row processing, rowtype = {0}".format(rf.rowtype))
    for r in rf:
        # r is a Dict representation of a data row
        ids_to_add = {}
        uuids_to_add = {}
        siblings = []
        try:
            if "id" in r:
                if r["id"] in unconsumed_extensions:
                    r.update(unconsumed_extensions[r["id"]])
                    del unconsumed_extensions[r["id"]]

                if r["id"] in core_siblings:
                    siblings = core_siblings[r["id"]]

            if rf.rowtype == "dwc:Occurrence" and "dwc:occurrenceID" not in r and "id" in r:
                r["dwc:occurrenceID"] = r["id"]
                del r["id"]

            etag = calcEtag(r)
            if etag in seen_etags:
                dupe_etags.add(etag)
                duplicate_record_count += 1
                raise RecordException("Duplicate ETag Detected, File {2}, Record {0}, Etag {1}".format(count,etag,rf.name))
            seen_etags.add(etag)

            proposed_idents = identifyRecord(rf.rowtype, etag, r, rsid)

            if config.IDB_EXTRA_SERIOUS_DEBUG == 'yes':
                rlogger.debug("****** proposed / identifyRecord = (etag, type of identifier, lowercased identifiers): {0}".format(proposed_idents))

            idents = []
            if len(proposed_idents) == 0 and rf.rowtype in ingestion_types:
                no_recordid_count += 1
                raise RecordException("No Record ID")
            elif len(proposed_idents) > 0:
                for ident in proposed_idents:
                    if config.IDB_EXTRA_SERIOUS_DEBUG == 'yes':
                        logger.debug("#### ident | ident[2] #### : %r | %r", ident, ident[2])
                    if ident[2] in seen_ids:
                        dupe_ids.add(ident[2])
                        duplicate_id_count += 1
                        raise RecordException(
                            "Duplicate ID Detected: {3}, File {2}, ID Record {0}, Previously Seen ID {1}".format(
                                count, ident[2], rf.name, ident))
                    else:
                        ids_to_add[ident[2]] = True
                        idents.append(ident)

            u = None
            matched = False
            for _, _, i in idents:
                if i in existing_ids:
                    if u is None:
                        found += 1
                        u = existing_ids[i]
                        if existing_etags[u] == etag:
                            match += 1
                            matched = True
                    else:
                        if existing_ids[i] != u:
                            raise RecordException("Cross record ID violation, ID {0}, UUID {1}".format(existing_ids[i], u))

            if config.IDB_EXTRA_SERIOUS_DEBUG == 'yes':
                rlogger.debug("****** idents = (etag, type of identifier, lowercased identifiers).................... {0}".format(idents))

            deleted = False
            if u is None:
                u, parent, deleted = db.get_uuid([i for _,_,i in idents])
                if parent is not None:
                    if parent != rsid:
                        if config.IDB_EXTRA_SERIOUS_DEBUG == 'yes':                       
                            rlogger.debug("******")
                            rlogger.debug("u: {0}".format(u))
                            rlogger.debug("parent: {0}".format(parent))
                            rlogger.debug("deleted: {0}".format(deleted))

                            rlogger.debug("****** Row:")
                            rlogger.debug("{0}".format(json.dumps(r)))
                        raise RecordException("UUID exists but has a parent other than expected. Expected parent (this recordset): {0}  Existing Parent: {1}  UUID: {2}".format(rsid,parent,u))

            if deleted:
                to_undelete += 1

            for _, _, i in idents:
                ids_to_add[i] = u
            uuids_to_add[u] = etag

            if ingest and not deleted:
                if matched:
                    # Always update siblings
                    for s in siblings:
                        db._upsert_uuid_sibling(u, s)
                else:
                    if config.IDB_EXTRA_SERIOUS_DEBUG == 'yes':
                        rlogger.debug("Setting sibling for '{0}'".format(u))
                    db.set_record(u, typ[:-1], rsid, r, ids_to_add.keys(), siblings)
                    ingestions += 1
            elif ingest and deleted:
                db.undelete_item(u)
                db.set_record(u, typ[:-1], rsid, r, ids_to_add.keys(), siblings)
                resurrections += 1


            # TODO:
            # It appears that archives are coming to us now with the 0'th column
            # named "id" instead of "coreId". This means the direct relationship from extension
            # to core is not being constructed for these rows.
            # TODO also:
            # What about the coreid vs coreId issue?
            if "coreid" in r:
                if rf.rowtype in ingestion_types:
                    if r["coreid"] in core_siblings:
                        core_siblings[r["coreid"]].append(u)
                    else:
                        core_siblings[r["coreid"]] = [u]
                else:
                    if r["coreid"] not in unconsumed_extensions:
                        unconsumed_extensions[r["coreid"]] = {}

                    if rf.rowtype not in unconsumed_extensions[r["coreid"]]:
                        unconsumed_extensions[r["coreid"]][rf.rowtype] = []

                    unconsumed_extensions[r["coreid"]][rf.rowtype].append(r)

            # TODO:
            # "ac:associatedSpecimenReference" currently gets checked even if we have already established
            # the related specimen record thru "strictor" relationship.  e.g. extension to coreid.
            # In cases where "ac:associatedSpecimenReference" points to a resource outside of iDigBio,
            # this causes a fatal error trying to process this row as no relationship can be built.
            if r.get("ac:associatedSpecimenReference"):
                ref_uuids_list = uuid_re.findall(r["ac:associatedSpecimenReference"])
                for ref_uuid in ref_uuids_list:
                    # Check for internal idigbio_uuid reference
                    db_r = db.get_item(ref_uuid)
                    db_uuid = None
                    if db_r is None:
                        # Check for identifier suffix match
                        # db_r will hold a tuple-ly thing.
                        db_r = db.fetchone(
                            "SELECT uuids_id FROM uuids_identifier WHERE reverse(identifier) LIKE reverse(%s)",
                            ("%" + ref_uuid,),cursor_factory=DictCursor)

                        if db_r is None:
                            db_uuid = None
                        else:
                            db_uuid = db_r["uuids_id"]

                    else:
                        db_uuid = db_r["uuid"]

                    if db_uuid is None:
                        # We probably need to do something other than raise this RecordException for this since 
                        # ac:associatedSpecimenReference is not required to be in *this* file / system.
                        # Can we find a different identifier?
                        raise RecordException("Record (idents: [{0}]) contains ac:associatedSpecimenReference '{1}' that does not relate to an existing identifier.".format(ref_uuid, idents))
                    elif ingest:
                        db._upsert_uuid_sibling(u, db_uuid)

            count += 1
        except RecordException as e:
            ids_to_add = {}
            uuids_to_add = {}
            rlogger.warn(e)
            record_exceptions += 1
        except AssertionError as e:
            ids_to_add = {}
            uuids_to_add = {}
            rlogger.warn(e)
            rlogger.info(traceback.format_exc())
            assertions += 1
        except DatabaseError as e:
            ids_to_add = {}
            uuids_to_add = {}
            rlogger.exception(e)
            dberrors += 1
        except Exception as e:
            ids_to_add = {}
            uuids_to_add = {}
            rlogger.exception("Uncaught exception handling %r", r)
            exceptions += 1

        seen_ids.update(ids_to_add)
        seen_uuids.update(uuids_to_add)

    eu_set = existing_etags.viewkeys()
    nu_set = seen_uuids.viewkeys()

    deletes = len(eu_set - nu_set)

    deleted = 0

    if ingest:
        for u in eu_set - nu_set:
            try:
                db.delete_item(u)
                deleted += 1
            except Exception:
                rlogger.info("Failed deleting %r", u, exc_info=True)

    return {
        "create": count - found,
        "update": found - match,
        "delete": deletes,
        "to_undelete": to_undelete,
        "ingestions": ingestions,
        "assertions": assertions,
        "resurrections": resurrections,
        "deleted": deleted,
        "processed_line_count": count,
        "total_line_count": rf.lineCount,
        "type": rf.rowtype,
        "no_recordid_count": no_recordid_count,
        "duplicate_record_count": duplicate_record_count,
        "duplicate_id_count": duplicate_id_count,
        "record_exceptions": record_exceptions,
        "exceptions": exceptions,
        "dberrors": dberrors,
        "processing_time": (datetime.datetime.now() - t).total_seconds()
    }


def process_file(fname, mime, rsid, existing_etags, existing_ids, ingest=False, commit_force=False, ispaused=False):
    rlogger = getrslogger(rsid)
    rlogger.info("Processing %s, type: %s", fname, mime)
    counts = {}
    t = datetime.datetime.now()
    filehash = calcFileHash(fname)
    db = PostgresDB()
    commited = False

    try:
        if mime == "application/zip":
            dwcaobj = Dwca(fname, skipeml=True, logname="idb")
            for dwcrf in dwcaobj.extensions:
                rlogger.debug("Processing %r", dwcrf.name)
                counts[dwcrf.name] = process_subfile(
                    dwcrf, rsid, existing_etags, existing_ids, ingest=ingest, db=db)
                dwcrf.close()
            rlogger.debug("processing core %r", dwcaobj.core.name)
            counts[dwcaobj.core.name] = process_subfile(
                dwcaobj.core, rsid, existing_etags, existing_ids, ingest=ingest, db=db)
            dwcaobj.core.close()
            dwcaobj.close()
        elif mime == "text/plain":
            commas = False
            with open(fname, "rb") as testf:
                commas = "," in testf.readline()

            if commas:
                csvrf = DelimitedFile(fname, logname="idigbio")
                counts[fname] = process_subfile(
                    csvrf, rsid, existing_etags, existing_ids, ingest=ingest, db=db)
            else:
                tsvrf = DelimitedFile(
                    fname, delimiter="\t", fieldenc=None, logname="idigbio")
                counts[fname] = process_subfile(
                    tsvrf, rsid, existing_etags, existing_ids, ingest=ingest, db=db)

        if ingest:
            commit_ok = commit_force

            type_commits = []
            for k in counts:
                if k not in ingestion_types:
                    continue
                if (
                    counts[k]["create"]/float(counts[k]["processed_line_count"]) >= 0.5 and
                    counts[k]["delete"]/float(counts[k]["processed_line_count"]) >= 0.5
                ):
                    type_commits.append(True)
                else:
                    type_commits.append(False)

            commit_ok = all(type_commits)

            if commit_ok:
                rlogger.info("Ready to Commit")
                db.commit()
                commited = True
            else:
                rlogger.error("Rollback")
                db.rollback()
        else:
            db.rollback()
        db.close()
    except Exception:
        logger.exception("Unhandled Exception when processing {0}".format(fname))
        db.rollback()
        db.close()

    # Clear after processing an archive
    unconsumed_extensions.clear()
    core_siblings.clear()

    return {
        "name": fname,
        "filemd5": filehash,
        "recordset_id": rsid,
        "counts": counts,
        "processing_start_datetime": t.isoformat(),
        "total_processing_time": (datetime.datetime.now() - t).total_seconds(),
        "commited": commited,
        "paused": ispaused
    }


def save_summary_json(rsid, counts):
    with AtomicFile(rsid + ".summary.json", "wb") as sumf:
        json.dump(counts, sumf, indent=2)


def metadataToSummaryJSON(rsid, metadata, writeFile=True, doStats=True):
    logger.info("%s writing summary json", rsid)
    summary = {
        "recordset_id": rsid,
        "filename": metadata["name"],
        "filemd5": metadata["filemd5"],
        "harvest_date": metadata["processing_start_datetime"],
        "records_count": 0,
        "records_create": 0,
        "records_update": 0,
        "records_delete": 0,
        "mediarecords_count": 0,
        "mediarecords_create": 0,
        "mediarecords_update": 0,
        "mediarecords_delete": 0,
        "datafile_ok": True,
        "commited": metadata["commited"],
        "paused": metadata["paused"]
    }

    if metadata["filemd5"] is None:
        summary["datafile_ok"] = False
        if writeFile:
            with AtomicFile(rsid + ".summary.json", "wb") as jf:
                json.dump(summary, jf, indent=2)
        return summary
    

    csv_line_count = 0
    no_recordid_count = 0
    duplicate_record_count = 0
    duplicate_id_count = 0
    for t in metadata["counts"].values():
        csv_line_count += t["total_line_count"]
        no_recordid_count += t["no_recordid_count"]
        duplicate_record_count += t["duplicate_record_count"]
        duplicate_id_count += t["duplicate_id_count"]
        if t["type"] in ingestion_types:
            typ = ingestion_types[t["type"]]
            summary[typ + "_count"] += t["processed_line_count"]

            for op in ["create", "update", "delete"]:
                if op in t:
                    summary[typ + "_" + op] += t[op]

    summary["csv_line_count"] = csv_line_count
    summary["no_recordid_count"] = no_recordid_count
    summary["duplicate_occurence_count"] = duplicate_record_count
    summary["dublicate_occurence_ids"] = duplicate_id_count

    if doStats:
        stats.index(doc_type='digest', body=summary)

    if writeFile:
        with AtomicFile(rsid + ".summary.json", "wb") as jf:
            json.dump(summary, jf, indent=2)
        with AtomicFile(rsid + ".metadata.json", "wb") as jf:
            json.dump(metadata, jf, indent=2)
    else:
        return summary


def main(rsid, ingest=False):
    from .db_rsids import get_paused_rsids
    paused_rsids = get_paused_rsids()

    with LoggingContext(
            filename='./{0}.db_check.log'.format(rsid),
            file_level=logging.DEBUG,
            clear_existing_handlers=False):
        rlogger = getrslogger(rsid)
        rlogger.info("Starting db_check ingest: %r", ingest)
        t = datetime.datetime.now()
        try:
            name, mime = get_file(rsid)
        except:
            rlogger.debug("Exception in get_file")
            # construct a dummy metadata record with no filemd5 so we can later write a summary file.
            metadata = {
                "name": rsid,
                "filemd5": None,
                "recordset_id": rsid,
                "counts": {
                    "create": 0,
                    "update": 0,
                    "delete": 0,
                    "to_undelete": 0,
                    "ingestions": 0,
                    "assertions": 0,
                    "resurrections": 0,
                    "deleted": 0,
                    "processed_line_count": 0,
                    "total_line_count": 0,
                    "type": 0,
                    "no_recordid_count": 0,
                    "duplicate_record_count": 0,
                    "duplicate_id_count": 0,
                    "record_exceptions": 0,
                    "exceptions": 1,
                    "dberrors": 0,
                    "processing_time": ""
                    },
                "processing_start_datetime": "",
                "total_processing_time": "",
                "commited": False,
                "paused": False,
                }
            metadataToSummaryJSON(rsid, metadata)
            rlogger.info("Finished db_check in %0.3fs", (datetime.datetime.now() - t).total_seconds())
            return rsid

        if os.path.exists(rsid + "_uuids.json") and os.path.exists(rsid + "_ids.json"):
            with open(rsid + "_uuids.json", "rb") as uuidf:
                db_u_d = json.load(uuidf)
            with open(rsid + "_ids.json", "rb") as idf:
                db_i_d = json.load(idf)
        else:
            rlogger.info("Building ids/uuids json")
            db_u_d, db_i_d = get_db_dicts(rsid)
            with AtomicFile(rsid + "_uuids.json", "wb") as uuidf:
                json.dump(db_u_d, uuidf)
            with AtomicFile(rsid + "_ids.json", "wb") as idf:
                json.dump(db_i_d, idf)

        commit_force = False
        if len(db_i_d) == 0 and len(db_u_d) == 0:
            commit_force = True

        ### At this point forward, we are already working on the rsid, so it is ok
        ### to mutate ingest directive on recordsets that are paused.
        ispaused = False
        if rsid in paused_rsids:
            ispaused = True
            rlogger.warn("Recordset is PAUSED. It will be checked but will not be ingested.".format(rsid))
            ingest = False

        metadata =process_file(name, mime, rsid, db_u_d, db_i_d, ingest=ingest, commit_force=commit_force, ispaused=ispaused)
        metadataToSummaryJSON(rsid, metadata)
        rlogger.info("Finished db_check in %0.3fs", (datetime.datetime.now() - t).total_seconds())
        return rsid


def launch_child(rsid, ingest):
    try:
        import logging
        # Close any logging filehandlers on root, leave alone any
        # other stream handlers (e.g. stderr) this way main can set up
        # its own filehandler to `$RSID.db_check.log`
        for fh in filter(lambda h: isinstance(h, logging.FileHandler), logging.root.handlers):
            logging.root.removeHandler(fh)
            fh.close()

        return main(rsid, ingest=ingest)
    except KeyboardInterrupt:
        logger.getChild(rsid).debug("KeyboardInterrupt in child")
        raise
    except Exception:
        logger.getChild(rsid).critical("Child failed", exc_info=True)


def allrsids(since=None, ingest=False):
    from .db_rsids import get_active_rsids
    rsids = get_active_rsids(since=since)
    logger.info("Checking %s recordsets", len(rsids))

    from .db_rsids import get_paused_rsids
    paused_recordsets = get_paused_rsids()
    logger.info("Paused recordsets: {0}, rsids: {1}".format(len(paused_recordsets),paused_recordsets))


    # Need to ensure all the connections are closed before multiprocessing forks
    apidbpool.closeall()

    pool = gipcpool.Pool()
    exitcodes = pool.imap_unordered(functools.partial(launch_child, ingest=ingest), rsids)
    badcount = ilen(e for e in exitcodes if e != 0)
    if badcount:
        logger.critical("%d children failed", badcount)
    from .ds_sum_counts import main as ds_sum_counts
    ds_sum_counts('./', sum_filename='summary.csv', susp_filename="suspects.csv")
