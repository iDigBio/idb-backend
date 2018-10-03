from __future__ import division, absolute_import
from __future__ import print_function

import json

from idb import config

from idb.postgres_backend.db import PostgresDB

def output_pretty(mylist):
    print("(Will become pretty printed...)")
    print(mylist)

def output_json(mylist):
    print(json.dumps(mylist))

def main():
    import argparse
    from idb.config import config

    parser = argparse.ArgumentParser(
        description='Print a report of newly discovered recordsets.')


    parser.add_argument('-f', '--format', dest='output_format',
                             help='Output format (default: pretty)', default='pretty',
                             choices=['pretty','json'])  # for now only support pretty (plain text) or JSON

    parser.add_argument('-a', '--age',
                             help='Number of days ago from today to mark beginning of "recent" period.', default=30, type=int)


    args = parser.parse_args()


    db = PostgresDB()

    sample_list = [{"key":"value"}]







    if args.output_format == "pretty":
        output_pretty(sample_list)

    if args.output_format == "json":
        output_json(sample_list)

if __name__ == "__main__":
    main()

