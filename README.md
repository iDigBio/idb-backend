# idb-backend

This is the collection of code that makes up the iDigBio server side.

## Installation

### System Dependencies

Currently this project only works in python2.7.

The following library packages will need to be installed to run the api:

In Ubuntu 14.04:

    apt-get install python2.7-dev libgeos-c1 libblas-dev liblapack-dev \
      libatlas-base-dev gfortran libgdal-dev

In Ubuntu 16.04:

    apt-get install python2.7-dev libblas-dev liblapack-dev \
      libatlas-base-dev gfortran libgdal-dev libpq-dev libgeos-c1v5 \
      libsystemd-dev


For ingestion the following are also needed:

In Ubuntu 14.04:

    apt-get install libxml2 libxslt1-dev libav-tools fonts-dejavu-core

In Ubuntu 16.04:

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


## Running

The main entry point is the `idb` command; you can run `idb --help` to
see what subcommands are available. When invoking this script there is
no need to set the `PYTHONPATH`.

## Testing

To run tests:

    py.test


Relies on having a local postgresql with user/pass `test` / `test`
that can connect to DB `test_idigbio`. The data in the DB will be
destroyed during the testing.

### Create the local DB

    createuser -l -e test -P
    createdb -l 'en_US.UTF-8' -E UTF8 -O test -e test_idigbio;

    # The schema obj is still owned by the user of the above
    # statement, not the owner 'test'. Drop it so it will be recreated
    # by the script appropriately
    psql -c "DROP SCHEMA public CASCADE;" test_idigbio


### Schema and data

Testing the DB uses the schema copied from the live DB with:

    pg_dump --host c18node8.acis.ufl.edu --username idigbio \
        --format plain --schema-only --schema=public \
        --clean --if-exists \
        --no-owner --no-privileges --no-tablespaces --no-unlogged-table-data \
        --file tests/data/schema.sql \
        idb_api_prod


The data has been built up to support the test suite; it is provided
in `tests/data/testdata.sql`

    pg_dump --port 5432 --format plain --data-only --encoding UTF8 \
      --inserts --column-inserts --no-privileges --no-tablespaces \
      --verbose --no-unlogged-table-data  \
      --file tests/data/testdata.sql $DBNAME
