from __future__ import division, absolute_import
from __future__ import print_function

import sys

import unicodecsv as csv

from idb import config

from idb.postgres_backend.db import PostgresDB

def output_tsv(mylist, outputfile):
    # tab-separated, all fields quoted
    rows = csv.writer(outputfile,quoting=csv.QUOTE_ALL,dialect='excel-tab')
    rows.writerows(mylist)

def main():
    import argparse
    from idb.config import config

    parser = argparse.ArgumentParser(
        description='Generate a report of newly discovered recordsets.')

    parser.add_argument('-a', '--age',
                             help='Number of days ago from today to mark beginning of "recent" period.', default=31, type=int)

    parser.add_argument('-w', '--write', action='store_true',
                             help='Write to a File instead of STDOUT.')


    args = parser.parse_args()


    db = PostgresDB()

    sql = """
       select uuid, name, publisher_uuid, file_link, to_json(first_seen), 
       to_json(pub_date), to_json(file_harvest_date) from recordsets where ingest=false and 
       first_seen > now()-'%s days'::interval order by first_seen"""  %  args.age

    results_list = db.fetchall(sql)

    if args.write:
        fh = open("fresh-recordsets-report.tsv","w")
        output_tsv(results_list, fh)
        fh.close()
    else:
        output_tsv(results_list, sys.stdout)


if __name__ == "__main__":
    main()

