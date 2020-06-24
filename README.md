# idb-backend

iDigBio server and backend code for data ingestion and data API.

## Installation

### System Dependencies

Currently this project only works in python2.7 and is not compatible with Python 3.

The following library packages will need to be installed to run the api:

In Ubuntu 16.04:

    apt-get install python2.7-dev libblas-dev liblapack-dev \
      libatlas-base-dev gfortran libgdal-dev libpq-dev libgeos-c1v5 \
      libsystemd-dev

In Ubuntu 18.04:

    apt install python-dev libblas-dev liblapack-dev \
      libatlas-base-dev gfortran libgdal-dev libpq-dev libgeos-c1v5 \
      libsystemd-dev

For Ingestion the following are also needed:

In Ubuntu 16.04, 18.04:

    apt-get install libxml2 libxslt1-dev ffmpeg fonts-dejavu-core


### Package installation

This package itself needs to be installed to generate the CLI
scripts. To install it editably (so that `git pull` continues to
updates everything) and include all dependencies:

    pip install -r requirements.txt

To setup persistent tasks and cron tasks

    systemctl link $PWD/etc/systemd/system/*
    systemctl enable --now $(grep -l Install $PWD/etc/systemd/system/*)


For partial installation (without extra components) you can just run

    pip install -e .

The available extra components are:

 * `journal`: Writes directly to systemd-journald
 * `ingestion`:  the extra librs for running ingestion
 * `test`: the extra libs for testing


### Docker Image

The [idb-backend](https://hub.docker.com/r/idigbio/idb-backend/) image
is built off of this repository.


## Running

The main entry points are the `idb` and `idigbio-ingestion` commands; you can run them with `--help` to
see what subcommands are available. When invoking this script there is
no need to set the `PYTHONPATH`.

In any invocation an `idigbio.json` must be present in either `$PWD`,
`$HOME`, or `/etc/idigbio`.

### Data API

This serves the `api.idigbio.org` interface

    idb run-server

### Celery worker

This package can also be run as a [celery worker]; this is used by the
data api that launches some tasks (most notably download building) via
celery to a background worker.

    celery worker --app=idigbio_workers -l WARNING

[celery worker]: http://docs.celeryproject.org/en/latest/userguide/workers.html

## Testing

To run tests:

    py.test


idb-backend testing relies on having a local postgresql with user/pass `test` / `test`
that can connect to DB `test_idigbio`. The data in the DB will be
destroyed during the testing.

### Create the local DB

    createuser -l -e test -P
    createdb -l 'en_US.UTF-8' -E UTF8 -O test -e test_idigbio;

    # The schema obj is still owned by the user of the above
    # statement, not the owner 'test'. Drop it so it will be recreated
    # by the script appropriately
    psql -c "DROP SCHEMA public CASCADE;" test_idigbio


### Schema

The live production db schema is copied into `tests/data/schema.sql` by periodically running this command:

    pg_dump --host c18node8.acis.ufl.edu --username idigbio \
        --format plain --schema-only --schema=public \
        --clean --if-exists \
        --no-owner --no-privileges --no-tablespaces --no-unlogged-table-data \
        --file tests/data/schema.sql \
        idb_api_prod

Except not yet because there are lots of differences between the existing file and one created by running that command (due to fixes for https://wiki.postgresql.org/wiki/A_Guide_to_CVE-2018-1058:_Protect_Your_Search_Path).


### Data

A trimmed down set of data has been manually curated to support the test suite. It is provided in `tests/data/testdata.sql`

The full dump / original was created with something like:

    pg_dump --port 5432 --host c18node8.acis.ufl.edu --username idigbio \
      --format plain --data-only --encoding UTF8 \
      --inserts --column-inserts --no-privileges --no-tablespaces \
      --verbose --no-unlogged-table-data  \
      --exclude-table-data=ceph_server_files \
      --file tests/data/testdata.sql idb_api_prod

Such a dump is huge and un-usable and un-editable by normal means. It is not clear how the dump was transformed / curated into its current state.

If running the dump again, consider adding multiple `--exclude-table-data=TABLE` for some of the bigger tables that are not materially relevant to test suite such as:

```plaintext
annotations
data
corrections
ceph_server_files
```
