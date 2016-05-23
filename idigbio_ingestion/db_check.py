from __future__ import absolute_import, print_function
import datetime
import functools
import json
import logging
import multiprocessing
import os
import re
import traceback

import magic
from atomicfile import AtomicFile
from psycopg2 import DatabaseError
from boto.exception import S3ResponseError, S3DataError


from idb import stats
from idb.postgres_backend import apidbpool
from idb.postgres_backend.db import PostgresDB, RecordSet
from idb.helpers.etags import calcEtag, calcFileHash
from idb.helpers.logging import idblogger, LoggingContext
from idb.helpers.storage import IDigBioStorage

from idigbio_ingestion.lib.dwca import Dwca
from idigbio_ingestion.lib.delimited import DelimitedFile



magic = magic.Magic(mime=True)

bad_chars = u"\ufeff"
bad_char_re = re.compile("[%s]" % re.escape(bad_chars))

logger = idblogger.getChild("db-check")

uuid_re = re.compile(
    "([a-fA-F0-9]{8}-[a-fA-F0-9]{4}-[a-fA-F0-9]{4}-[a-fA-F0-9]{4}-[a-fA-F0-9]{12})")

s = IDigBioStorage()

class RecordException(Exception):
    pass


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
        RecordSet.fetch_file(rsid, fname, media_store=IDigBioStorage(), logger=logger.getChild(rsid))

    mime = magic.from_file(fname)
    return (fname, mime)


def get_db_dicts(rsid):
    id_uuid = {}
    uuid_etag = {}
    for t in ["record", "mediarecord"]:
        id_uuid[t + "s"] = {}
        uuid_etag[t + "s"] = {}
        for c in PostgresDB.get_children_list(rsid, t, limit=None):
            u = c["uuid"]
            e = c["etag"]
            uuid_etag[t + "s"][u] = e
            for i in c["recordids"]:
                id_uuid[t + "s"][i] = u
    return (uuid_etag, id_uuid)


def identifyRecord(t, etag, r, rsid):
    idents = []
    if t in identifier_fields:
        for pi in identifier_fields[t]:
            if pi[0] in r:
                # The "UConn" exception
                if pi[0] == "ac:providerManagedID" and "dcterms:identifier" in r and r["dcterms:identifier"].lower() == r["ac:providerManagedID"].lower():
                    continue
                cid = pi[1](r[pi[0]], rsid)
                if cid is not None:
                    idents.append((etag, pi[0], cid.lower()))
                    # Breaking would give us only the first ID
                    # break
    return idents

unconsumed_extensions = {}
core_siblings = {}


def process_subfile(rf, rsid, rs_uuid_etag, rs_id_uuid, ingest=False, db=None):
    rlogger = logger.getChild(rsid)

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
    for r in rf:
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

            idents = []
            if len(proposed_idents) == 0 and rf.rowtype in ingestion_types:
                no_recordid_count += 1
                raise RecordException("No Record ID")
            elif len(proposed_idents) > 0:
                for ident in proposed_idents:
                    if ident[2] in seen_ids:
                        dupe_ids.add(ident[2])
                        duplicate_id_count += 1
                        raise RecordException("Duplicate ID Detected, File {2}, Record {0}, ID {1}".format(count,ident[2],rf.name))
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
                            raise RecordException("Cross record ID violation")

            deleted = False
            if u is None:
                u, parent, deleted = db.get_uuid([i for _,_,i in idents])
                assert u is not None
                if parent is not None:
                    # assert parent == rsid
                    if parent != rsid:
                        raise RecordException("Record exists but has a parent other than expected. Expected parent (this recordset): {0}  Existing Parent: {1}  Record: {2}".format(rsid,parent,u))

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
                    #             u, t,        p,    d, ids,               siblings, commit
                    # print u, typ[:-1], rsid, r, ids_to_add.keys(), siblings
                    db.set_record(u, typ[:-1], rsid, r, ids_to_add.keys(), siblings)
                    ingestions += 1
            elif ingest and deleted:
                db.undelete_item(u)
                db.set_record(u, typ[:-1], rsid, r, ids_to_add.keys(), siblings)
                resurrections += 1

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

            if "ac:associatedSpecimenReference" in r and r["ac:associatedSpecimenReference"] is not None:
                ref_uuids_list = uuid_re.findall(r["ac:associatedSpecimenReference"])
                for ref_uuid in ref_uuids_list:
                    # Check for internal idigbio_uuid reference
                    db_r = db.get_item(ref_uuid)
                    db_uuid = None
                    if db_r is None:
                        # Check for identifier suffix match
                        db_r = db.fetchone(
                            "SELECT uuids_id FROM uuids_identifier WHERE reverse(identifier) LIKE reverse(%s)",
                            ("%" + ref_uuid,))
                        db_uuid = db_r["uuids_id"]
                    else:
                        db_uuid = db_r["uuid"]

                    if db_uuid is not None and ingest:
                        db._upsert_uuid_sibling(u, db_uuid)

            count += 1
        except RecordException as e:
            ids_to_add = {}
            uuids_to_add = {}
            rlogger.warn(e)
            rlogger.info(traceback.format_exc())
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
            rlogger.warn(e)
            rlogger.error(traceback.format_exc())
            exceptions += 1

        seen_ids.update(ids_to_add)
        seen_uuids.update(uuids_to_add)

        # if ingestions % 10000 == 0:
        #     db.commit()

    eu_set = existing_etags.viewkeys()
    nu_set = seen_uuids.viewkeys()

    deletes = len(eu_set - nu_set)

    deleted = 0

    if ingest:
        for u in eu_set - nu_set:
            try:
                db.delete_item(u)
                deleted += 1
            except:
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


def process_file(fname, mime, rsid, existing_etags, existing_ids, ingest=False, commit_force=False):
    rlogger = logger.getChild(rsid)
    rlogger.info("Processing %s, type: %s", fname, mime)
    counts = {}
    t = datetime.datetime.now()
    filehash = calcFileHash(fname)
    db = PostgresDB()

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

    commited = False
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
    }

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
    with LoggingContext(
            filename='./{0}.db_check.log'.format(rsid),
            file_level=logging.DEBUG,
            clear_existing_handlers=False):
        rlogger = logger.getChild(rsid)
        rlogger.info("Starting db_check ingest: %r", ingest)
        t = datetime.datetime.now()
        try:
            name, mime = get_file(rsid)
        except (S3ResponseError, S3DataError):
            rlogger.exception("failed fetching archive")
            raise

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

        metadata = process_file(name, mime, rsid, db_u_d, db_i_d, ingest=ingest, commit_force=commit_force)
        metadataToSummaryJSON(rsid, metadata)
        rlogger.info("Finished db_check in %0.3fs", (datetime.datetime.now() - t).total_seconds())
        return rsid


def launch_child(rsid, ingest):
    try:
        import logging
        # close any logging filehandlers on root, leave alone any
        # other stream handlers (e.g. stderr) this way main can set up
        # its own filehandler to `$RSID.db_check.log`
        for fh in list(filter(lambda h: isinstance(h, logging.FileHandler), logging.root.handlers)):
            logging.root.removeHandler(fh)
            fh.close()

        return main(rsid, ingest=ingest)
    except KeyboardInterrupt:
        logger.debug("Child KeyboardInterrupt")
        pass
    except:
        logger.getChild(rsid).critical("Child failed", exc_info=True)


def allrsids(since=None, ingest=False):
    from .db_rsids import get_active_rsids

    rsids = get_active_rsids(since=since)
    logger.info("Checking %s recordsets", len(rsids))

    # Need to ensure all the connections are closed before multiprocessing forks
    apidbpool.closeall()

    pool = multiprocessing.Pool()
    try:
        results = list(
            pool.imap_unordered(
                functools.partial(launch_child, ingest=ingest),
                rsids))
    except KeyboardInterrupt:
        logger.debug("Got KeyboardInterrupt")
        raise
    from .ds_sum_counts import main as ds_sum_counts
    ds_sum_counts('./', sum_filename='summary.csv', susp_filename="suspects.csv")
