# iDigBio Ingestion processes #

This is a collection of scripts and helpers for ingesting data into
iDigBio.

## Running Commands ##

All the commands are entered through the top level `idigbio-ingestion`
script. When invoking this script there is no need to set the
`PYTHONPATH`.

### running ingestion ###

* `idigbio-ingestion update-publisher-recordset`: This is the update
  publisher script that checks for new versions.
* `idigbio-ingestion db-check $RSID`: Check a dataset against the DB
  and report what will be ingested
* `idigbio-ingestion ingest $RSID`: Run ingestion proper on the given dataset
* `idigbio-ingestion db-check-all [--since="$DATE"]`: Check all
  datasets against the DB and report what will be ingested; this also
  generates the summary and suspects reports in the current directory.
* `idigbio-ingestion ingest-all [--since="$DATE"]`: Ingest all
  datasets; this also generates the summary and suspects reports in
  the current directory.

### Media ###

* `idigbio-ingestion mediaing [--last-check-interval="$INTERVAL"] updatedb`: Update the DB with new URLs
* `idigbio-ingestion mediaing get-media`: Fetch the actual media
* `idigbio-ingestion derivatives`
