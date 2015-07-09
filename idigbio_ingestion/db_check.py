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

# import logging
# logger = logging.getLogger()
# logger.setLevel(logging.DEBUG)

from idb.postgres_backend.db import PostgresDB
from idb.helpers.etags import calcEtag, calcFileHash

from lib.dwca import Dwca
from lib.delimited import DelimitedFile

db = PostgresDB()
magic = magic.Magic(mime=True)

INGESTION_TYPES = ["record", "mediarecord"]

bad_chars = u"\ufeff"
bad_char_re = re.compile("[%s]" % re.escape(bad_chars))

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
        r = requests.get("https://media.idigbio.org/lookup/datasets", params={
                         "filereference": "http://api.idigbio.org/v1/recordsets/" + rsid}, stream=True)
        r.raise_for_status()
        with open(fname, 'wb') as f:
            r.raw.decode_content = True
            shutil.copyfileobj(r.raw, f)
    m = magic.from_file(fname)
    return (fname, m)


def get_db_dicts(rsid):
    id_uuid = {}
    uuid_etag = {}
    for t in INGESTION_TYPES:
        print t
        for c in db.get_children_list(rsid, t, limit=None):
            u = c["uuid"]
            e = c["etag"]
            uuid_etag[u] = e
            for i in c["recordids"]:
                id_uuid[i] = u
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


def process_subfile(rf, rsid, existing_etags, existing_ids):
    count = 0
    no_recordid_count = 0
    duplicate_record_count = 0
    duplicate_id_count = 0

    seen_etags = set()
    seen_ids = set()

    dupe_ids = set()
    dupe_etags = set()

    found = 0
    match = 0

    t = datetime.datetime.now()
    for r in rf:
        try:
            if "id" in r and r["id"] in unconsumed_extensions:
                r.update(unconsumed_extensions[r["id"]])
                del unconsumed_extensions[r["id"]]

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
                my_ids = set()
                for ident in proposed_idents:
                    if ident[2] in my_ids:
                        # Skip duplicate ids within a single record without
                        # error.
                        continue
                    elif ident[2] in seen_ids:
                        dupe_ids.add(ident[2])
                        duplicate_id_count += 1
                        raise RecordException("Duplicate ID Detected")
                    else:
                        my_ids.add(ident[2])
                        seen_ids.add(ident[2])
                        idents.append(ident)

            u = None
            for _, _, i in idents:
                if i in existing_ids:
                    if u is None:
                        found += 1
                        u = existing_ids[i]
                        if existing_etags[u] == etag:
                            match += 1
                    else:
                        if existing_ids[i] != u:
                            raise Exception("Cross record ID violation")

            if "coreid" in r and rf.rowtype not in ingestion_types:
                if r["coreid"] not in unconsumed_extensions:
                    unconsumed_extensions[r["coreid"]] = {}

                if rf.rowtype not in unconsumed_extensions[r["coreid"]]:
                    unconsumed_extensions[r["coreid"]][rf.rowtype] = []

                unconsumed_extensions[r["coreid"]][rf.rowtype].append(r)

            count += 1
        except RecordException as e:
            #logger.error(str(e) + ", File: " + rf.name + " Line: " + str(rf.lineCount))
            #logger.debug(traceback.format_exc())
            pass
        except Exception as e:            
            traceback.print_exc()
    return {
        "found": found,
        "match": match,
        "processed_line_count": count,
        "total_line_count": rf.lineCount,
        "type": rf.rowtype,
        "no_recordid_count": no_recordid_count,
        "duplicate_record_count": duplicate_record_count,
        "duplicate_id_count": duplicate_id_count,
        "processing_time": (datetime.datetime.now() - t).total_seconds()
    }


def process_file(fname, mime, rsid, existing_etags, existing_ids):
    counts = {}

    if mime == "application/zip":
        dwcaobj = Dwca(fname, skipeml=True)
        for dwcrf in dwcaobj.extensions:
            counts[dwcrf.name] = process_subfile(
                dwcrf, rsid, existing_etags, existing_ids)
            dwcrf.close()
        counts[dwcaobj.core.name] = process_subfile(
            dwcaobj.core, rsid, existing_etags, existing_ids)
        dwcaobj.core.close()
        dwcaobj.close()
    elif mime == "text/plain":
        commas = False
        with open("fname", "rb") as testf:
            commas = "," in testf.readline()

        if commas:
            csvrf = DelimitedFile(fname)
            counts[fname] = process_subfile(
                csvrf, rsid, existing_etags, existing_ids)
        else:
            tsvrf = DelimitedFile(fname, delimiter="\t", fieldenc=None)
            counts[fname] = self.processRecordFile(tsvrf)

    # Clear after processing an archive
    unconsumed_extensions = {}

    return counts

def save_summary_json(rsid,counts):
    with open(rsid + ".summary.json", "wb") as sumf:
        json.dump(counts, sumf, indent=2)

def main():
    rsid = sys.argv[1]
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
    counts = process_file(name, mime, rsid, db_u_d, db_i_d)
    save_summary_json(rsid,counts)

if __name__ == '__main__':
    main()
