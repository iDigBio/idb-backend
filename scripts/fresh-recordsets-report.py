from __future__ import division, absolute_import
from __future__ import print_function


from idb import config

from idb.postgres_backend.db import PostgresDB


def main():
    import argparse
    from idb.config import config

    parser = argparse.ArgumentParser(
        description='Print a report of newly discovered recordsets.')


    parser.add_argument('-f', '--format', dest='output_format',
                             help='Output format (default: pretty)', default='pretty',
                             choices=['pretty','json'])  # for now only support pretty (plain text) or JSON


    args = parser.parse_args()

    #db = PostgresDB()

    print (args)

if __name__ == "__main__":
    main()

