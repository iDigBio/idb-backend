from __future__ import division, absolute_import
from __future__ import print_function

import click
from gevent import monkey

from idb.clibase import cli, clilog


@cli.command(name="update-publisher-recordset",
             help="With no args updates all, with uuids updates "
             "only those publishers or recordsets")
#@click.argument('uuids', nargs=-1)
def update_publisher_recordset():
    from idigbio_ingestion.update_publisher_recordset import main
    main()


@cli.group(help="Fetch/update applicable media")
@click.option("--tropicos/--no-tropicos", default=False,
              help="Enable special tropicos logic")
@click.option("--urlfilter", default=None)
@click.option("--last-check-interval", default=None)
@click.pass_context
def mediaing(ctx, tropicos, urlfilter, last_check_interval):
    ctx.obj = params = {}
    from idigbio_ingestion import mediaing
    if tropicos:
        # This module import changes the behavior or the `mediaing` module.
        from idigbio_ingestion import mediaing_tropicos  # noqa
        params['urlfilter'] = mediaing_tropicos.TROPICOS_URLFILTER
    if urlfilter:
        params['urlfilter'] = urlfilter

    if last_check_interval:
        mediaing.LAST_CHECK_INTERVAL = last_check_interval


@mediaing.command(name="get-media")
@click.pass_obj
def mediaing_get_media(mediaing_params):
    from idigbio_ingestion.mediaing import get_media_consumer
    clilog.debug("params: {!r}", mediaing_params)
    get_media_consumer(**mediaing_params)


@mediaing.command(name="updatedb")
@click.argument('urlfilter', required=False)
def mediaing_updatedb(urlfilter):
    from idigbio_ingestion.mediaing import \
        get_postgres_media_urls, write_urls_to_db
    media_urls = get_postgres_media_urls(urlfilter)
    write_urls_to_db(media_urls)
    pass


@cli.command(name="db-check",
             help="Check a dataset, by rsid, against the database "
             "and report what would have been ingested")
@click.option("--ingest/--no-ingest", default=False)
@click.argument("rsid", type=click.UUID)
def db_check(ingest, rsid):
    rsid = u'{0}'.format(rsid)
    from idigbio_ingestion.db_check import main
    main(rsid, ingest=ingest)


@cli.command(name="db-check-all",
             help="Run db-check in parallel against all datasets")
@click.option("--since", help="Only check sets harvested since the given date")
def db_check_all(since):
    from idigbio_ingestion.db_check import all
    all(since)


@cli.command(name="ingest",
             help="Ingest a dataset, by rsid")
@click.option("--ingest/--no-ingest", default=False)
@click.argument("rsid", type=click.UUID)
def db_check_ingest(rsid):
    rsid = u'{0}'.format(rsid)
    from idigbio_ingestion.db_check import main
    main(rsid, ingest=True)


@cli.command(name="db-rsids")
def db_rsids():
    from idigbio_ingestion.db_rsids import main3
    main3()


@cli.command(help="Generate derivatives in the specified buckets."
             " Buckets currently can be {'images', 'sounds'}."
             " Defaults to both.")
@click.argument('bucket', nargs=-1)
def derivatives(bucket):
    monkey.patch_all()
    from idigbio_ingestion.derivatives import main
    main(bucket)


@cli.command(name="ds-sum-counts", help="Generate summary and suspect counts")
@click.argument('base_dir', default='./',
                type=click.Path(exists=True, dir_okay=True, file_okay=False))
@click.argument('summary_filename', default='summary.csv',
                type=click.Path(dir_okay=False))
@click.argument('suspects_filename', default='suspects.csv',
                type=click.Path(dir_okay=False))
def ds_sum_counts(base_dir, summary_filename, suspects_filename):
    from idigbio_ingestion.ds_sum_counts import main
    main(base_dir, summary_filename, suspects_filename)




#def run_checks(name="run-checks", help="Report what would be ingested")
