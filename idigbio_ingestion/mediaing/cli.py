from __future__ import division, absolute_import, print_function

import click
from gevent import monkey

from idb.clibase import cli
from idb.helpers.logging import fnlogged


@cli.group(help="Fetch/update applicable media")
@click.option("--tropicos/--no-tropicos", default=False,
              help="Enable special tropicos logic")
@click.option("--prefix", default=None,
              help="limit to urls with this prefix")
@click.pass_context
@fnlogged
def mediaing(ctx, tropicos, prefix):
    if prefix and '%' in prefix:
        click.echo("Don't use wildcards in prefix", err=True)
        click.abort()

    ctx.obj = params = {'prefix': prefix}
    monkey.patch_all()
    from idigbio_ingestion import mediaing  # noqa
    if tropicos:
        # This module import changes the behavior or the `mediaing` module.
        from idigbio_ingestion.mediaing import tropicos  # noqa
        params['prefix'] = tropicos.TROPICOS_PREFIX


@mediaing.command(name="get-media", help="Fetch the actual media")
@click.option("--last-check-interval", default=None,
              help="Postgres interval for minimum time since the media url was last checked.")
@click.pass_obj
@fnlogged
def mediaing_get_media(mediaing_params, last_check_interval=None):
    from idigbio_ingestion.mediaing import fetcher
    if last_check_interval:
        mediaing.LAST_CHECK_INTERVAL = last_check_interval
    fetcher.get_media_consumer(prefix=mediaing_params['prefix'])


@mediaing.command(name="updatedb", help="Update the DB with new URLs")
@click.pass_obj
@fnlogged
def mediaing_updatedb(mediaing_params):
    from idigbio_ingestion.mediaing import updatedb
    updatedb.updatedb(prefix=mediaing_params['prefix'])


@cli.command(help="Generate derivatives in the specified buckets."
             " Buckets currently can be {'images', 'sounds'}."
             " Defaults to both.")
@click.option('--continuous/--no-continuous', default=False,
              help="Run derivatives continuously w/o exiting")
@click.argument('bucket', nargs=-1)
@fnlogged
def derivatives(continuous, bucket):
    monkey.patch_all()
    from idigbio_ingestion.mediaing import derivatives
    if continuous:
        derivatives.continuous(bucket)
    else:
        derivatives.main(bucket)


@cli.command(name="migrate-media-objects", help="Migrate database entries from old media api table.")
@fnlogged
def migrate_media_objects():
    from idigbio_ingestion.mediaing.derivatives import migrate
    migrate()
