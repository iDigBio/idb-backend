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

For Ingestion and Development, the following are also needed:

In Ubuntu 16.04, 18.04:

    apt-get install libxml2 libxslt1-dev ffmpeg fonts-dejavu-core libfreetype6-dev


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

## Development and Testing

You probably want to run in a virtual environment.   You may wish to disable the pip cache
to verify package builds are working properly.

```bash
$ virtualenv -p python2.7 .venv

$ source .venv/bin/activate

$ python --version
Python 2.7.17

$ pip --no-cache-dir install -e .

$ pip --no-cache-dir install -r requirements.txt
```

It is possible in the future that this project will be runnable using "Open in container" features of Microsoft Visual Studio Code (aka vscode or just `code`).

### Testing Dependencies

Some idb-backend tests depend on external resources, such as a database or Elasticsearch.

A local postgresql DB named `test_idigbio` with user/pass `test` / `test` must exist for many of the tests to run.  Note: The data in the DB will be destroyed during the testing.

Database tests will be SKIPPED if the database is not available.

Tests that depend on Elasticsearch will FAIL if the computer running the tests cannot reach the Elasticsearch cluster (fail very slowly in fact), or if there is some other failure.

### Running tests

The test suite can be run by executing `py.test` (or `pytest`).

However due to the dependencies mentioned above, you may wish to run the database in docker each time.  The sleep is needed to allow postgres time to start accepting connections.

    docker run --rm --name postgres_test_idigbio --network host \
      -e POSTGRES_PASSWORD=test -e POSTGRES_USER=test -e POSTGRES_DB=test_idigbio  \
      -d postgres:9.5 && \
      sleep 5; \
      py.test ; \
      docker stop postgres_test_idigbio
        

To exclude a single set of tests that are failing (or Seg Faulting!), add the `--deselect` option to the pytest command:

    py.test --deselect=tests/idigbio_ingestion/mediaing/test_derivatives.py

To find out why tests are being Skipped, add the `-rxs` options.

A "what the heck is going on with the tests and skip the one that is Seg Faulting" example command:

    docker run --rm --name postgres_test_idigbio --network host  \
      -e POSTGRES_PASSWORD=test -e POSTGRES_USER=test -e POSTGRES_DB=test_idigbio \
      -d postgres:9.5 && \
      sleep 5; \
      py.test -rxs --deselect=tests/idigbio_ingestion/mediaing/test_derivatives.py ; \
      docker stop postgres_test_idigbio

### Create a local postgres DB

The recommended approach is to run postgres via docker (see above).

If you have a full installation of postgres running locally, the db can be manually created with: 

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

We likely need to find a new way to refresh the test dataset.
