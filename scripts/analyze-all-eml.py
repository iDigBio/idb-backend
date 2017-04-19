from __future__ import division, absolute_import, print_function

from idb import config
from idb.postgres_backend.db import PostgresDB
from idigbio_ingestion.lib.eml import parseEml
from idigbio_ingestion.lib.util import download_file
import sys
import os


def analyze_and_save_eml(ident, url):
    "Download an eml file, run parseEml against it, return results as a list."
    if download_file(url, ident):
        intext = open(ident, 'r').read()
        try:
            result = parseEml(ident, intext)["data_rights"]
            os.unlink(ident)
            return [ident, url, result]
        except:
            # Comment the next line to parse exceptions around for further analysis
            os.unlink(ident)
            return [ident, url, "EXCEPTION"]
    else:
        return [ident, url, 'ERROR']



if __name__ == '__main__':
    statuses = {}

    abort_one = False

    with PostgresDB() as db:
        rows = db.fetchall("SELECT id,eml_link FROM recordsets where ingest=true")

        for emlz in rows:
            results = analyze_and_save_eml(str(emlz[0]), emlz[1])
            statuses[results[0]] = [results[1],results[2]]
            print ('{0} {1}'.format(results[0], statuses[results[0]]))
            if abort_one:
                break

    outfilename = 'analyze-all-eml.output'

    with open(outfilename, 'w') as outfile:
        sorted_status_keys = sorted(statuses.keys())
        for status in sorted_status_keys:
            outfile.write("{0},'{1}','{2}'\n".format(status, statuses[status][0], statuses[status][1]))
        print ("Sorted output file: ", outfilename)

