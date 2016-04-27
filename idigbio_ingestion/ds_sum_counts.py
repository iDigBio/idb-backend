"""New version of ds_sum_counts that generates both summary and suspects

"""
from __future__ import division, absolute_import
from __future__ import print_function

import logging

import json
import os
import sys
import io

from collections import Counter


header = [
    "file", "csv line cnt", "R cnt", "R cr8", "R upd8",
    "R del", "MR cnt", "MR cr8", "MR upd8",
    "MR del", "dup occ ids", "no recordid cnt"
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

    if records_count == 0 and mediarecords_count == 0:
        return "NO_RECORDS"

    if records_count > 0 and records_create == records_count and records_update == 0 and records_delete == 0:
        return "ALLNEW_RECORDS"
    if records_count == 0 and records_delete > 0:
        return "DELETED_ALL_RECORDS"
    if records_count > 0:
        if records_delete / records_count > 0.05:
            return "DELETED_MANY_RECORDS"
        if records_create / records_count > 0.4:
            return "MANY_NEW_RECORDS"
    if records_delete > 0 and 0.9 < (records_create / records_delete) < 1.1:
        return "RECORDS_CHURN"

    if mediarecords_count > 0 and mediarecords_create == mediarecords_count and mediarecords_update == 0 and mediarecords_delete == 0:
        return "ALLNEW_MEDIA"
    if mediarecords_count == 0 and mediarecords_delete > 0:
        return "DELETED_ALL_MEDIA"
    if mediarecords_count > 0:
        if mediarecords_delete / mediarecords_count > 0.05:
            return "DELETED_MANY_MEDIA"
        if mediarecords_create / mediarecords_count > 0.4:
            return "MANY_NEW_MEDIA"
    if mediarecords_delete > 0 and 0.9 < (mediarecords_create / mediarecords_delete) < 1.1:
        return "MEDIA_CHURN"

    return False


def main(base, sum_filename, susp_filename):
    summary_data = read_all_files(base)
    summary_data = sorted(summary_data, key=lambda r: r['filename'])

    suspect_rows = []
    totals = Counter()

    with io.open(sum_filename, 'w', encoding='utf-8') as fp:
        write_header(fp)
        for row in summary_data:
            write_row(fp, row)
            for fld in fields:
                totals[fld] += row.get(fld, 0)
            suspect_tag = is_row_suspect(row)
            if suspect_tag:
                row['tag'] = suspect_tag
                suspect_rows.append(row)
        write_row(fp, totals, 'totals')

    logging.debug("Found %d suspect rows", len(suspect_rows))

    with io.open(susp_filename, 'w', encoding='utf-8') as fp:
        if len(suspect_rows) > 0:
            header.insert(1, 'tag')
            fields.insert(1, 'tag')
            write_header(fp)
            for row in suspect_rows:
                write_row(fp, row)


if __name__ == '__main__':
    base, sum_filename, susp_filename = sys.argv[1:]
    main(base, sum_filename, susp_filename)
