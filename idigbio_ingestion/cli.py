from __future__ import division, absolute_import
from __future__ import print_function

import click
import json

from idb.clibase import cli
from idb.helpers.logging import fnlogged

from .mediaing import cli as mcli  # noqa ignore=F401

@cli.command(name="update-publisher-recordset",
             help="")
@fnlogged
def update_publisher_recordset():
    from idigbio_ingestion.update_publisher_recordset import main
    main()

@cli.command(name="upload-recordset-from-file",
             help="Run the upload step for a single local dataset file manually. "
             "This skips all RSS feed processing.")
@click.argument("rsid", type=click.UUID)
@click.argument("file", type=click.Path())
@fnlogged
def manual_update_recordset_from_file(rsid, file):
    from idigbio_ingestion.update_publisher_recordset import upload_recordset_from_file
    upload_recordset_from_file(rsid, file)


@cli.command(name="db-check",
             help="Check a dataset, by rsid, against the database "
             "and report what will be ingested")
@click.argument("rsid", type=click.UUID)
@fnlogged
def db_check(rsid):
    rsid = u'{0}'.format(rsid)
    from idigbio_ingestion.db_check import main
    main(rsid, )


@cli.command(name="db-check-file",
             help="Check a dataset, by filename, against the database "
             "and report what will be ingested")
@click.argument("file", type=click.Path())
@click.option("--csv", is_flag=True, default=False, help="Process file as a CSV instead of a DwCA (zip)")
@fnlogged
def db_check_file(file, csv=False):
    # rsid = u'{0}'.format(rsid)
    from idigbio_ingestion.db_check import process_file
    if csv:
        mime="text/plain"
    else:
        mime="application/zip"
    print(json.dumps(process_file(
        file,
        mime,
        "00000000-0000-0000-0000-000000000000",
        {"records":{},"mediarecords":{}},
        {"records":{},"mediarecords":{}},
        False,
        False
    ), indent=2))


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
