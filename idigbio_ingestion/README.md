# iDigBio Ingestion processes #

This is a collection of scripts and helpers for ingesting data into
iDigBio.

## New Ingestion Procedure

0. If there are new publishers, add them with new [Adding a new Publisher procedure][].
0. Change to the data directory (currently `/mnt/data/new_ingestion/` on c18node4) and remove the previous ingestion run contents.
0. Run `update-publisher-recordset` to verify all publishers and recordsets are in known good state.
0. Run the `db-check-all` subcommand
0. Verify errors, possibly need to make correction and re-run.
0. Inspect the counts in the summary report files, especially the suspects file.
0. If everything looks good, run the `ingest-all` subcommand to perform actual ingestion of records into the database
0. Start indexing of new records via `index_from_postgres.py` with the `-k` full check option.
0. While indexing is running it is safe to run the `mediaing` commands. First to insert new urls into the database, then to download new media
   * There are services on c18node4 that run these continuously
0. Run the `derivatives` command to generate thumbnails
   * There is a service on c18node4 that runs this continuously
0. Validate that recordset, records, and media appear in the portal
0. Update redmine tickets
0. Update Data Ingestion Report on the iDigBio wiki


[Adding a new Publisher procedure]: https://www.idigbio.org/redmine/projects/infrastructure/wiki/Adding_a_new_Publisher

## Command information ##

All the commands are entered through the top level `idigbio-ingestion`
script. When invoking this script there is no need to set the
`PYTHONPATH`.

Notable toplevel options for `idigbio-ingestion`

* `-v`: verbose, can be repeated for extra verbosity
* `--config=$PATH`: read a `idigbio.json` file at the given path.
* `--logfile=$PATH`: write a logfile at the given location.

### Recordset Ingestion ###

The following subcommands are related to recordset ingestion

* `update-publisher-recordset`: This is the update
  publisher script that checks for new versions.
* `db-check $RSID`: Check a dataset against the DB
  and report what will be ingested
* `ingest $RSID`: Run ingestion proper on the given dataset
* `db-check-all [--since="$DATE"]`: Check all
  datasets against the DB and report what will be ingested. This
  should be run in the ingestion data directory and will also
  generates the summary and suspects reports in the current directory.
* `ingest-all [--since="$DATE"]`: Ingest all
  datasets. This should be run in the ingestion data directory and
  will also generates the summary and suspects reports in the current
  directory.
* `ds-sum-counts`: Generate summary and

### Media Ingestion ###

All of these commands are run automatically/continuously on c18node4
managed by systemd (See section below on automated tasks). They can
however also continue to be invoked manually.

* `mediaing`:
  * Common options:
    * `--prefix`: limit to urls w/ this prefix
  * commands:
    * `updatedb`: Update the DB with new URLs
    * `get-media [--last-check-interval="$INTERVAL"]`: Fetch the
      actual media, if specified only get

* `derivatives [{images, sounds}*]`: takes arguments
  of which buckets to run from, if none specified then all known
  buckets will be used

To see latest status/log messages use `systemctl status
idigbio-ingestion-$CMD.service`. Tab-completion should be active.

### Automated tasks ###

All the automated tasks are managed by systemd, with the configuration
stored in etc/systemd/system.

To see a list of the timers, and when they will next trigger run
`systemctl list-timers 'idb*' 'idigbio*'`. The details of an
individual timer, or the service it triggers can be seen with
`systemctl status $TIMER/SERVICE`.
