from __future__ import division, absolute_import, print_function
from datetime import datetime

import click

from idb.clibase import cli
from idb.helpers.logging import fnlogged

indexName = "stats"
typeName = "search"


def index(index=indexName, body=None, doc_type=typeName, es=None):
    if es is None:
        from idb.indexing.indexer import get_connection
        es = get_connection()
    return es.index(index=index, doc_type=doc_type, body=body)


def search(index=indexName, body=None, doc_type=typeName, es=None):
    if es is None:
        from idb.indexing.indexer import get_connection
        es = get_connection()
    return es.query(index=index, body=body, doc_type=doc_type)


@cli.command(name="collect-stats",
             help="Collect the stats for a day from postgres, defaults to yesterday")
@click.option('--date', '-d', default=datetime.now().isoformat(),
              help="date to collect for, '*' for all dates")
@click.option('--mapping/--no-mapping', default=False, help="write mapping")
@fnlogged
def collect_stats(date, mapping):
    import dateutil.parser
    from . import collect

    # This builds the schema mapping that's needed when initially creating the index.
    # for example output of this function, see: ./example-objects/stats-index-mapping.json
    if mapping:
        collect.put_search_stats_mapping()

    if date == '*':
        for d in collect.get_stats_dates():
            collect.collect_stats(d)
    else:
        collect_datetime = dateutil.parser.parse(date)
        collect.collect_stats(collect_datetime)


@cli.command(name="api-stats", help="write out the api stats")
@click.option('--mapping/--no-mapping', default=False, help="write mapping")
@fnlogged
def api_stats(mapping):
    from . import collect
    if mapping:
        collect.put_search_stats_mapping()
    collect.api_stats()
