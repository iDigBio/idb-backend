from __future__ import division, absolute_import
from __future__ import print_function

import sys

import json
import unicodecsv as csv
import pprint

from idb import config

from idb.postgres_backend.db import PostgresDB


def output_pretty(mylist):
    # this isn't very pretty actually.
    pp = pprint.PrettyPrinter()
    pp.pprint(mylist)

def output_json(mylist):
    print(json.dumps(mylist))

def output_csv(mylist):
    # tab-separated, all fields quoted
    rows = csv.writer(sys.stdout,quoting=csv.QUOTE_ALL,dialect='excel-tab')
    rows.writerows(mylist)

def main():
    import argparse
    from idb.config import config

    parser = argparse.ArgumentParser(
        description='Print a report of newly discovered recordsets.')


    parser.add_argument('-f', '--format', dest='output_format',
                             help='Output format (default: pretty)', default='pretty',
                             choices=['pretty','json','csv'])

    parser.add_argument('-a', '--age',
                             help='Number of days ago from today to mark beginning of "recent" period.', default=31, type=int)


    args = parser.parse_args()


    db = PostgresDB()

    sql = """
       select uuid, name, publisher_uuid, file_link, to_json(first_seen), 
       to_json(pub_date), to_json(file_harvest_date) from recordsets where ingest=false and 
       first_seen > now()-'%s days'::interval order by first_seen"""  %  args.age

    results_list = db.fetchall(sql)


    if args.output_format == "pretty":
        output_pretty(results_list)

    if args.output_format == "json":
        output_json(results_list)

    if args.output_format == "csv":
        output_csv(results_list)

if __name__ == "__main__":
    main()

