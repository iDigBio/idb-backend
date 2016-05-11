"""This package helps index from the bulk datastore to an efficient
indexing engine.

Right now this is from postgres to elasticsearch.

"""

from __future__ import division, absolute_import
from __future__ import print_function


import click

from idb.clibase import cli as _cli
from idb import config
from idb.helpers.logging import idblogger

log = idblogger.getChild('indexing')


@_cli.group(name="index", help="indexing into ElasticSearch")
@click.option('--index/--no-index',
              default=True,
              help="Enable/disable posting to elasticsearch, Default: enabled")
@click.option('--types', '-t',
              # TODO: this should porbably be `idb.helpers.conversions.fields.keys()`
              type=click.Choice(['records', 'recordsets', 'mediarecords', 'publishers']),
              multiple=True)
@click.option('--indexname')
@click.pass_context
def cli(ctx, index, types, indexname):
    from .indexer import ElasticSearchIndexer
    from idb.corrections.record_corrector import RecordCorrector

    if not types:
        types = config.config["elasticsearch"]["types"]
    if indexname is None:
        indexname = config.config["elasticsearch"]["indexname"]
    serverlist = config.config["elasticsearch"]["servers"]

    if config.ENV == 'beta':
        log.info("Enabling beta configuration")
        indexname = "2.5.0"
        serverlist = [
            "c17node52.acis.ufl.edu",
            "c17node53.acis.ufl.edu",
            "c17node54.acis.ufl.edu",
            "c17node55.acis.ufl.edu",
            "c17node56.acis.ufl.edu"
        ]

    if not index:
        log.info("Enabling no-index dry run mode")

    # These are the parameters that are common to every indexing
    # function
    ctx.obj = {
        'ei': ElasticSearchIndexer(indexname, types, serverlist=serverlist),
        'rc': RecordCorrector(),
        'no_index': not index
    }

    # indexing monitors for SIGUSR1 output for logging but the default
    # python handler causes exit on SIGUSR1
    from signal import signal, SIGUSR1, SIG_IGN
    signal(SIGUSR1, SIG_IGN)


@cli.command(help="run incremental continously")
@click.pass_obj
def continuous(params):
    from .index_from_postgres import continuous_incremental
    continuous_incremental(**params)


@cli.command(help="run incremental index")
@click.pass_obj
def incremental(params):
    from .index_from_postgres import incremental
    incremental(**params)


@cli.command(help="Index data matching ES query")
@click.argument('query')
@click.pass_obj
def query(params, query):
    import json
    from .index_from_postgres import _query
    _query(query=json.loads(query), **params)


@cli.command(name='uuid-file',
             help="Index the uuids listed in the specified file, one per line")
@click.argument('uuid-file', type=click.File())
@click.pass_obj
def uuid_file(params, uuid_file):
    from .index_from_postgres import uuids
    lines = [l for l in
             (l.strip() for l in uuid_file)
             if l]
    uuids(uuid_l=lines, **params)


@cli.command(help="Index the specified uuids")
@click.argument('uuid', nargs=-1)
@click.pass_obj
def uuids(params, uuid):
    from .index_from_postgres import uuids
    uuids(uuid_l=uuid, **params)


@cli.command(help="resume a full sync (full + etag compare)")
@click.pass_obj
def resume(params):
    from .index_from_postgres import resume
    resume(**params)


@cli.command(help="run a full sync")
@click.pass_obj
def full(params):
    from .index_from_postgres import full
    full(**params)


@cli.command(help="delete records from index that are deleted in api")
@click.pass_obj
def delete(params):
    from .index_from_postgres import delete
    delete(**params)


@cli.command(help="run a full check (delete + resume)")
@click.pass_obj
def check(params):
    from .index_from_postgres import resume
    resume(also_delete=True, **params)
