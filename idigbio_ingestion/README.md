# iDigBio Ingestion processes #

This is a collection of scripts and helpers for ingesting data into
iDigBio.

## Ingestion Procedure

1. If there are new publishers, add them with new
   [Adding a new Publisher procedure][]
2. Run the `db-check-all` subcommand
3. Verify errors, possibly need to make correction and re-run.
4. Restart idigbio-api-service on each api node to help avoid memory leak
5. Run the `ingest-all` subcommand
6. Verify errors, possibly need to make correction and re-run.
7. If recordsets included media run the `mediaing` and `derivatives` commands
8. Validate that recordset, records, and media appear in the portal
9. Update redmine tickets
10. Update Data Ingestion Report on the iDigBio wiki

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

* `mediaing`:
  * Common options:
    * `--tropicos`: enable custom tropicos logic
    * `--prefix`: limit to urls w/ this prefix
  * commands:
    * `updatedb`: Update the DB with new URLs
    * `get-media [--last-check-interval="$INTERVAL"]`: Fetch the
      actual media, if specified only get

* `derivatives [{images, sounds}*]`: takes arguments
  of which buckets to run from, if none specified then all known
  buckets will be used
