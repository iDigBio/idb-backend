from __future__ import division, absolute_import
from __future__ import print_function

import click
from gevent import monkey

from idb.clibase import cli, clilog
from idb.helpers.logging import fnlogged

@cli.command(name="update-publisher-recordset",
             help="")
@fnlogged
def update_publisher_recordset():
    from idigbio_ingestion.update_publisher_recordset import main
    main()


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
        from idigbio_ingestion import mediaing_tropicos  # noqa
        params['prefix'] = mediaing_tropicos.TROPICOS_PREFIX


@mediaing.command(name="get-media", help="Fetch the actual media")
@click.option("--last-check-interval", default=None,
              help="Postgres interval for minimum time since the media url was last checked.")
@click.pass_obj
@fnlogged
def mediaing_get_media(mediaing_params, last_check_interval=None):
    from idigbio_ingestion import mediaing
    if last_check_interval:
        mediaing.LAST_CHECK_INTERVAL = last_check_interval
    mediaing.get_media_consumer(prefix=mediaing_params['prefix'])


@mediaing.command(name="updatedb", help="Update the DB with new URLs")
@click.pass_obj
@fnlogged
def mediaing_updatedb(mediaing_params):
    from idigbio_ingestion.mediaing import updatedb
    updatedb(prefix=mediaing_params['prefix'])


@cli.command(help="Generate derivatives in the specified buckets."
             " Buckets currently can be {'images', 'sounds'}."
             " Defaults to both.")
@click.option('--continuous/--no-continuous', default=False,
              help="Run derivatives continuously w/o exiting")
@click.argument('bucket', nargs=-1)
@fnlogged
def derivatives(continuous, bucket):
    monkey.patch_all()
    from idigbio_ingestion import derivatives
    if continuous:
        derivatives.continuous(bucket)
    else:
        derivatives.main(bucket)


@cli.command(name="migrate-media-objects", help="Migrate database entries from old media api table.")
@fnlogged
def migrate_media_objects():
    from idigbio_ingestion.derivatives import migrate
    migrate()

@cli.command(name="db-check",
             help="Check a dataset, by rsid, against the database "
             "and report what will be ingested")
@click.argument("rsid", type=click.UUID)
@fnlogged
def db_check(rsid):
    rsid = u'{0}'.format(rsid)
    from idigbio_ingestion.db_check import main
    main(rsid, )


@cli.command(name="db-check-all", help="Run db-check against all datasets")
@click.option("--since",
              help="Only check recordsets harvested since the given date; e.g. YYYY-MM-DD")
@fnlogged
def db_check_all(since):
    from idigbio_ingestion.db_check import allrsids
    allrsids(since)


@cli.command(name="ingest", help="Ingest a dataset, by rsid")
@click.argument("rsid", type=click.UUID)
@fnlogged
def ingest(rsid):
    rsid = u'{0}'.format(rsid)
    from idigbio_ingestion.db_check import main
    main(rsid, ingest=True)


@cli.command(name="ingest-all", help="Ingest all datasets")
@click.option("--since", help="Only ingest sets harvested since the given date")
@fnlogged
def ingest_all(since):
    from idigbio_ingestion.db_check import allrsids
    allrsids(since, ingest=True)


@cli.command(name="db-rsids",
             help="Print the list of all recordset UUIDs that should be ingested")
@fnlogged
def db_rsids():
    from idigbio_ingestion.db_rsids import main3
    main3()


@cli.command(name="ds-sum-counts", help="Generate summary and suspect counts")
@click.argument('base_dir', default='./',
                type=click.Path(exists=True, dir_okay=True, file_okay=False))
@click.argument('summary_filename', default='summary.csv',
                type=click.Path(dir_okay=False))
@click.argument('suspects_filename', default='suspects.csv',
                type=click.Path(dir_okay=False))
@fnlogged
def ds_sum_counts(base_dir, summary_filename, suspects_filename):
    from idigbio_ingestion.ds_sum_counts import main
    main(base_dir, summary_filename, suspects_filename)
