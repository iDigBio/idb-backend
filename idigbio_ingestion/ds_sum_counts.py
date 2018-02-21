"""New version of ds_sum_counts that generates both summary and suspects

"""
from __future__ import division, absolute_import
from __future__ import print_function

import json
import os
import io
import subprocess
from collections import Counter
from atomicfile import AtomicFile

from idb.helpers.logging import idblogger as logger


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
            p = os.path.join(base, f)
            with open(p, "rb") as fp:
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

    if not row.get('datafile_ok'):
        return "DATAFILE_NOT_OK"

    if records_count == 0 and mediarecords_count == 0:
        return "NO_RECORDS"

    if records_count > 0 and records_create == records_count and records_update == 0 and records_delete == 0:
        return "ALLNEW_RECORDS"
    if records_count == 0 and records_delete > 0:
        return "DELETED_ALL_RECORDS"
    if records_delete > 0 and \
       0.9 < (records_create / records_delete) < 1.1 and \
       records_create / records_count > 0.1:
        return "RECORDS_CHURN"
    if records_count > 0:
        if records_delete / records_count > 0.2:
            return "DELETED_MANY_RECORDS"
        if records_create / records_count > 0.45:
            return "MANY_NEW_RECORDS"

    if mediarecords_count > 0 and mediarecords_create == mediarecords_count and mediarecords_update == 0 and mediarecords_delete == 0:
        return "ALLNEW_MEDIA"
    if mediarecords_count == 0 and mediarecords_delete > 0:
        return "DELETED_ALL_MEDIA"
    if mediarecords_count > 0:
        if mediarecords_delete / mediarecords_count > 0.05:
            return "DELETED_MANY_MEDIA"
        if mediarecords_create / mediarecords_count > 0.4:
            return "MANY_NEW_MEDIA"
    if mediarecords_delete > 0 and \
       0.9 < (mediarecords_create / mediarecords_delete) < 1.1 and \
       mediarecords_create / mediarecords_count > 0.1:
        return "MEDIA_CHURN"

    return False


def main(base, sum_filename, susp_filename):
    summary_data = read_all_files(base)

    summary_data = sorted(summary_data, key=lambda r: r['filename'])
    logger.info("Read in %d *.summary.json files", len(summary_data))
    suspect_rows = []
    totals = Counter()

    logger.info("Generating... %s", sum_filename)
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

    logger.info("Found %d suspect rows", len(suspect_rows))

    with io.open(susp_filename, 'w', encoding='utf-8') as fp:
        if len(suspect_rows) > 0:
            header.insert(1, 'tag')
            fields.insert(0, 'tag')  # the fields don't include the filename column
            write_header(fp)
            for row in suspect_rows:
                write_row(fp, row)

    sum_pretty_filename = sum_filename.replace('.csv', '.pretty.txt')
    susp_pretty_filename = susp_filename.replace('.csv', '.pretty.txt')
    logger.info("Converting %s (all recordsets subject to ingestion) to columnar human readable report... %s",
                sum_filename, sum_pretty_filename)
    columnize(sum_filename, sum_pretty_filename)

    logger.info("Converting %s (recordsets with questionable updates) to columnar human readable report... %s",
                susp_filename, susp_pretty_filename)
    columnize(susp_filename, susp_pretty_filename)

def columnize(ifile, ofile):
    #column -ts ',' summary.csv | sort > summary.pretty.txt
    p = subprocess.Popen(['column', '-ts', ',', ifile], stdout=subprocess.PIPE)
    lines = p.stdout.readlines()
    with AtomicFile(ofile, 'w', encoding='utf-8') as out:
        for l in lines:
            out.write(l)
