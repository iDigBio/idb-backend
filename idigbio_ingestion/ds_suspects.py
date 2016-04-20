"""New version of ds_sum_counts that generates both summary and suspects

We don't fully trust this script yet, so it doesn't write out
summary.csv; but it does generate it and compare with an existing one
and logs a warning if it wouldn't match.

"""
from __future__ import division, absolute_import
from __future__ import print_function

import logging

import json
import os
import sys
import io

from collections import Counter
from idb.helpers.etags import calcFileHash


header = [
    "file", "csv line count", "record count", "record create", "record update",
    "record delete", "mediarecord count", "mediarecord create", "mediarecord update",
    "mediarecord delete", "duplicated occurence ids", "no recordid count"
]

fields = [
    "csv_line_count", "records_count", "records_create", "records_update",
    "records_delete", "mediarecords_count", "mediarecords_create",
    "mediarecords_update", "mediarecords_delete", "duplicate_occurence_count",
    "no_recordid_count"
]


def read_all_files(base):
    for f in os.listdir(base):
        if f.endswith(".summary.json"):
            with open(base + f, "rb") as fp:
                o = json.load(fp)
                yield o


def write_header(fp):
    fp.write(u",".join(header))
    fp.write(u"\n")

def write_row(fp, row, k=None):
    if k is None:
        k = row['filename']

    values = [str(row.get(fld, 0)) for fld in fields]
    fp.write(k + u"," + u",".join(values))
    fp.write(u"\n")


def is_row_suspect(row):
    records_count = row.get('records_count', 0)
    records_create = row.get('records_create', 0)
    records_update = row.get('records_update', 0)
    records_delete = row.get('records_delete', 0)
    mediarecords_count = row.get('mediarecords_count', 0)
    mediarecords_create = row.get('mediarecords_create', 0)
    mediarecords_update = row.get('mediarecords_update', 0)
    mediarecords_delete = row.get('mediarecords_delete', 0)

    if records_count == 0:
        return True
    if records_delete / records_count > 0.05:
        return True
    if records_create / records_count > 0.4:
        return True
    if records_delete > 0:
        if 0.9 < (records_create / records_delete) < 1.1:
            return True

    if mediarecords_delete > 0:
        if mediarecords_count == 0:
            return True
        if mediarecords_delete / mediarecords_count > 0.05:
            return True

    return False


def filehashcompare(fp, diskname='summary.csv'):
    try:
        fp.seek(0)
        newetag = calcFileHash(fp, op=False)
        oldetag = calcFileHash(diskname, op=True)
        if newetag != oldetag:
            logging.warning("proposed summary.csv generation != existing")
    except IOError:
        logging.warning("Failed has comparison", exc_info=True)
        return


def main(base, sum_filename, susp_filename):
    summary_data = read_all_files(base)
    summary_data = sorted(summary_data, key=lambda r: r['filename'])

    suspect_rows = []
    totals = Counter()

    with io.StringIO() as fp:
        write_header(fp)
        for row in summary_data:
            write_row(fp, row)
            for fld in fields:
                totals[fld] += row.get(fld, 0)
            if is_row_suspect(row):
                suspect_rows.append(row)
        write_row(fp, totals, 'totals')
        filehashcompare(fp, sum_filename)

    logging.debug("Found %d suspect rows", len(suspect_rows))

    with io.open(susp_filename, 'w', encoding='utf-8') as fp:
        if len(suspect_rows) > 0:
            totals = Counter()
            write_header(fp)
            for row in suspect_rows:
                write_row(fp, row)
                for fld in fields:
                    totals[fld] += row.get(fld, 0)
            write_row(fp, totals, 'totals')


if __name__ == '__main__':
    base, sum_filename, susp_filename = sys.argv[1:]
    main(base, sum_filename, susp_filename)
