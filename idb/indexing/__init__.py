"""This package helps index from the bulk datastore to an efficient
indexing engine.

Right now this is from postgres to elasticsearch.

"""
from __future__ import division, absolute_import, print_function

import click

from idb.clibase import cli as _cli
from idb.helpers.logging import fnlogged


@_cli.group(name="index", help="indexing into ElasticSearch")
@click.option('--index/--no-index',
              default=True,
              help="Enable/disable posting to elasticsearch, Default: enabled")
@click.option('--corrections/--no-corrections',
              default=True,
              help="Enable/disable the record corrector, Default: enabled")
@click.option('--types', '-t',
              # TODO: this should probably be `idb.helpers.conversions.fields.keys()`
              type=click.Choice([
                  'publishers', 'recordsets', 'mediarecords', 'records']),
              multiple=True)
@click.option('--indexname')
@click.pass_context
@fnlogged
def cli(ctx, index, corrections, types, indexname):
    from idb.helpers.logging import idblogger
    logger = idblogger.getChild('indexing')
    from idb import config
    from .indexer import ElasticSearchIndexer
    from idb.corrections.record_corrector import RecordCorrector

    if not types:
        types = config.config["elasticsearch"]["types"]
    if indexname is None:
        indexname = config.config["elasticsearch"]["indexname"]
    serverlist = config.config["elasticsearch"]["servers"]

    if config.ENV == 'beta':
        logger.info("Enabling beta configuration")
        indexname = "2.5.0"
        serverlist = [
            "c17node52.acis.ufl.edu",
            "c17node53.acis.ufl.edu",
            "c17node54.acis.ufl.edu",
            "c17node55.acis.ufl.edu",
            "c17node56.acis.ufl.edu"
        ]

    if not index:
        logger.info("Enabling no-index dry run mode")

    # These are the parameters that are common to every indexing
    # function
    ctx.obj = {
        'ei': lambda: ElasticSearchIndexer(indexname, types, serverlist=serverlist),
        'rc': lambda: RecordCorrector(reload=corrections),
        'no_index': lambda: not index
    }


@cli.command(help="run incremental continously")
@click.pass_obj
@fnlogged
def continuous(params):
    from .index_from_postgres import continuous_incremental
    for k in params:
        params[k] = params[k]()
    continuous_incremental(**params)


@cli.command(help="run incremental index")
@click.pass_obj
@fnlogged
def incremental(params):
    from .index_from_postgres import incremental
    for k in params:
        params[k] = params[k]()
    incremental(**params)


@cli.command(help="Index data matching ES query")
@click.argument('query')
@click.pass_obj
@fnlogged
def query(params, query):
    import json
    from .index_from_postgres import _query
    for k in params:
        params[k] = params[k]()
    _query(query=json.loads(query), **params)


@cli.command(name='uuid-file',
             help="Index the uuids listed in the specified file, one per line")
@click.argument('uuid-file', type=click.File())
@click.pass_obj
@fnlogged
def uuid_file(params, uuid_file):
    from .index_from_postgres import uuids
    lines = [l for l in
             (l.strip() for l in uuid_file)
             if l]
    for k in params:
        params[k] = params[k]()
    uuids(uuid_l=lines, **params)


@cli.command(help="Index the specified uuids")
@click.option('--children', is_flag=True, default=False, help="Index the children of specified uuids")
@click.argument('uuid', nargs=-1)
@click.pass_obj
@fnlogged
def uuids(params, children, uuid):
    from .index_from_postgres import uuids
    for k in params:
        params[k] = params[k]()
    uuids(children=children, uuid_l=uuid, **params)


@cli.command(help="resume a full sync (full + etag compare)")
@click.pass_obj
@fnlogged
def resume(params):
    from .index_from_postgres import resume
    for k in params:
        params[k] = params[k]()
    resume(**params)


@cli.command(help="run a full sync")
@click.pass_obj
@fnlogged
def full(params):
    from .index_from_postgres import full
    for k in params:
        params[k] = params[k]()
    full(**params)


@cli.command(help="delete records from index that are deleted in api")
@click.pass_obj
@fnlogged
def delete(params):
    from .index_from_postgres import delete
    for k in params:
        params[k] = params[k]()
    delete(**params)


@cli.command(help="run a full check (delete + resume)")
@click.pass_obj
@fnlogged
def check(params):
    from .index_from_postgres import resume
    for k in params:
        params[k] = params[k]()
    resume(also_delete=True, **params)
