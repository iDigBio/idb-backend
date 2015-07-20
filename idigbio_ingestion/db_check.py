import os
import sys
import uuid
import gc
import re
import datetime
import traceback

import requests
import shutil
import magic
import json

import logging
from lib.log import getIDigBioLogger, formatter

from idb.postgres_backend.db import PostgresDB
from idb.helpers.etags import calcEtag, calcFileHash

from lib.dwca import Dwca
from lib.delimited import DelimitedFile
from lib.util import download_file


from idb.stats_collector import es, indexName

db = PostgresDB()
magic = magic.Magic(mime=True)

bad_chars = u"\ufeff"
bad_char_re = re.compile("[%s]" % re.escape(bad_chars))

logger = getIDigBioLogger("idigbio")
logger.setLevel(logging.ERROR)


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
    ]
}

ingestion_types = {
    "dwc:Occurrence": "records",
    "dwc:Multimedia": "mediarecords",
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
        download_file("https://media.idigbio.org/lookup/datasets", fname, params={
                         "filereference": "http://api.idigbio.org/v1/recordsets/" + rsid})
    m = magic.from_file(fname)
    return (fname, m)


def get_db_dicts(rsid):
    id_uuid = {}
    uuid_etag = {}
    for t in ["record", "mediarecord"]:
        id_uuid[t + "s"] = {}
        uuid_etag[t + "s"] = {}
        for c in db.get_children_list(rsid, t, limit=None):
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


def process_subfile(rf, rsid, rs_uuid_etag, rs_id_uuid, ingest=False):
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
                raise RecordException("Duplicate ETag Detected")
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
                        raise RecordException("Duplicate ID Detected")
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
                    assert parent == rsid

            if deleted = True:
                to_undelete += 1

            for _, _, i in idents:
                ids_to_add[i] = u
            uuids_to_add[u] = etag

            if ingest and not matched and not deleted:
                #             u, t,        p,    d, ids,               siblings, commit
                db.set_record(u, typ[:-1], rsid, r, ids_to_add.keys(), siblings, commit=False)
                ingestions += 1
            elif ingest and deleted:
                db.undelete_item(u, commit=False)
                db.set_record(u, typ[:-1], rsid, r, ids_to_add.keys(), siblings, commit=False)
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

            count += 1
        except RecordException as e:
            ids_to_add = {}
            uuids_to_add = {}
            #logger.error(str(e) + ", File: " + rf.name + " Line: " + str(rf.lineCount))
            # logger.debug(traceback.format_exc())
            # traceback.print_exc()
            pass
        except AssertionError as e:
            ids_to_add = {}
            uuids_to_add = {}
            traceback.print_exc()
            assertions += 1
        except Exception as e:
            ids_to_add = {}
            uuids_to_add = {}
            traceback.print_exc()

        seen_ids.update(ids_to_add)
        seen_uuids.update(uuids_to_add)

        # if ingestions % 10000 == 0:
        #     db.commit()

    eu_set = existing_etags.viewkeys()
    nu_set = seen_uuids.viewkeys()

    deletes = len(eu_set - nu_set)

    # ei_set = existing_ids.viewkeys()
    # ni_set = seen_ids.viewkeys()

    # ee_set = set(existing_etags.values())



    # if ingest:
    #     print typ, "Create UUIDS", len(nu_set), len(eu_set), len(nu_set - eu_set)
    #     for nu in nu_set - eu_set:
    #         print "MOCK INSERT INTO uuids (id, type, parent, deleted) VALUES ({0},{1},{2},{3})".format(nu, typ[:-1], rsid, False)

    #     print typ, "Create Idents", len(ni_set), len(ei_set), len(ni_set - ei_set)
    #     for ni in ni_set - ei_set:
    #         print "MOCK INSERT INTO uuids_identifier (identifier, uuids_id) VALUES ({0},{1})".format(ni, seen_ids[ni])

    #     print typ, "Create Etags", len(seen_etags), len(ee_set), len(seen_etags - ee_set), len(etags_to_ingest)
    #     # for ne in etags_to_ingest:
    #     #     print "MOCK INSERT INTO data (etag, data) VALUES ({0},{1})".format(ne,etags_to_ingest[ne][1])
    #     # print "MOCK INSERT INTO uuids_data (uuids_id,data_etag) VALUES
    #     # ({0},{1})".format(etags_to_ingest[ne][0],ne)

    return {
        "create": count - found,
        "update": found - match,
        "delete": deletes,
        "to_undelete": to_undelete,
        "ingestions": ingestions,
        "assertions": assertions,
        "resurrections": resurrections,
        "processed_line_count": count,
        "total_line_count": rf.lineCount,
        "type": rf.rowtype,
        "no_recordid_count": no_recordid_count,
        "duplicate_record_count": duplicate_record_count,
        "duplicate_id_count": duplicate_id_count,
        "processing_time": (datetime.datetime.now() - t).total_seconds()
    }


def process_file(fname, mime, rsid, existing_etags, existing_ids, ingest=False, commit_force=False):
    counts = {}
    t = datetime.datetime.now()
    filehash = calcFileHash(fname)

    if mime == "application/zip":
        dwcaobj = Dwca(fname, skipeml=True, logname="idigbio")
        for dwcrf in dwcaobj.extensions:
            counts[dwcrf.name] = process_subfile(
                dwcrf, rsid, existing_etags, existing_ids, ingest=ingest)
            dwcrf.close()
        counts[dwcaobj.core.name] = process_subfile(
            dwcaobj.core, rsid, existing_etags, existing_ids, ingest=ingest)
        dwcaobj.core.close()
        dwcaobj.close()
    elif mime == "text/plain":
        commas = False
        with open(fname, "rb") as testf:
            commas = "," in testf.readline()

        if commas:
            csvrf = DelimitedFile(fname, logname="idigbio")
            counts[fname] = process_subfile(
                csvrf, rsid, existing_etags, existing_ids, ingest=ingest)
        else:
            tsvrf = DelimitedFile(
                fname, delimiter="\t", fieldenc=None, logname="idigbio")
            counts[fname] = process_subfile(
                tsvrf, rsid, existing_etags, existing_ids, ingest=ingest)

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
            print "Ready to Commit"
            db.commit()
            commited = True
        else:
            db.rollback()

    # Clear after processing an archive
    unconsumed_extensions = {}
    core_siblings = {}

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
    with open(rsid + ".summary.json", "wb") as sumf:
        json.dump(counts, sumf, indent=2)


def metadataToSummaryJSON(rsid, metadata, writeFile=True, doStats=True):
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
        es.index(index=indexName,doc_type="digest",body=summary)

    if writeFile:
        with open(rsid + ".summary.json", "wb") as jf:
            json.dump(summary, jf, indent=2)
        with open(rsid + ".metadata.json", "wb") as jf:
            json.dump(metadata, jf, indent=2)
    else:
        return summary


def main():
    rsid = sys.argv[1]
    ingest = False
    if len(sys.argv) > 2:
        # RSID is always last for xargs support
        rsid = sys.argv[-1]
        ingest = sys.argv[1] == "ingest"

    fh = logging.FileHandler(rsid + ".log")
    fh.setLevel(logging.INFO)
    fh.setFormatter(formatter)
    logger.addHandler(fh)

    name, mime = get_file(rsid)
    if os.path.exists(rsid + "_uuids.json") and os.path.exists(rsid + "_ids.json"):
        with open(rsid + "_uuids.json", "rb") as uuidf:
            db_u_d = json.load(uuidf)
        with open(rsid + "_ids.json", "rb") as idf:
            db_i_d = json.load(idf)
    else:
        db_u_d, db_i_d = get_db_dicts(rsid)
        with open(rsid + "_uuids.json", "wb") as uuidf:
            json.dump(db_u_d, uuidf)
        with open(rsid + "_ids.json", "wb") as idf:
            json.dump(db_i_d, idf)
    metadata = process_file(name, mime, rsid, db_u_d, db_i_d, ingest=ingest)
    metadataToSummaryJSON(rsid, metadata)

if __name__ == '__main__':
    main()
