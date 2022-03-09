# idb-backend

[![Build Status](https://travis-ci.com/iDigBio/idb-backend.svg?branch=master)](https://travis-ci.com/iDigBio/idb-backend)

iDigBio server and backend code for data ingestion and data API.

## Installation

### System Dependencies

Currently this project only works in Python 2.7 and is not compatible with Python 3 (development work towards compatibility with python3 is underway in the `convert_python_3` branch).

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

    apt-get install libxml2 libxslt1-dev ffmpeg fonts-dejavu-core libfreetype6-dev python-systemd


### Package installation

This package itself needs to be installed to generate the CLI
scripts. To install it editably (so that `git pull` continues to
updates everything) and include all dependencies:

    pip install -r requirements.txt


For partial installation (without extra components) you can just run

    pip install -e .

The available extra components are:

 * `ingestion`:  the extra librs for running ingestion
 * `test`: the extra libs for testing

### Persistent Services

#### data api

The data API is currently the only way external to the database to view previous versions of resources (e.g. v1 of a record that has been updated multiple times).

The service unit file is `idigbio-data-api.service`.

#### stats aggregator

Stats of various type are aggregated by this service.  More info is available in [the stats readme](stats/readme.md).

#### data ingestion services

The ingestion services are configured to run as a user named `idigbio-ingestion`.  If this user does not exist on the system it must be created.  It is suggested to use a home directory `/home/idigbio-ingestion`.


1. Install the `virtualenv` system package (and the Dependencies listed above if not already installed):

```
apt install virtualenv

apt install python-dev libblas-dev liblapack-dev \
    libatlas-base-dev gfortran libgdal-dev libpq-dev libgeos-c1v5 \
    libsystemd-dev \
    libxml2 libxslt1-dev ffmpeg fonts-dejavu-core libfreetype6-dev python-systemd
```

2. Become the idigbio-ingestion user (via `su - idigbio-ingestion`), and clone this repo:

    git clone https://github.com/iDigBio/idb-backend.git

3. Set up the python virtual environment:

```
cd idb-backend
virtualenv -p python2.7 .venv
source .venv/bin/activate
python --version
pip --no-cache-dir install -e .
pip --no-cache-dir install -r requirements.txt
deactivate
```

From this point, software inside the virtual environment (including python, pip, py.test, etc.) can be run by referencing the path to the binary inside the environment.

    ./venv/bin/pip freeze

4. configure idb-backend config

Place a valid `idigbio.json` in the `idigbio-ingestion` home directory.


5. Setup the ingestion-related services, by linking systemd to the unit files included in the repo.

*Note:* All of the following `systemctl` commands will need to be run by root or a user with sudo permission. 

    systemctl link /home/idigbio-ingestion/etc/systemd/system/idigbio-ingestion-*

6. Enable the services as needed.

    systemctl list-units idigbio-ingestion-*

    systemctl enable <service>

The recommended services to enable and start:

    idigbio-ingestion-update-publisher-recordset.timer

    idigbio-ingestion-mediaing-get-media.service

    idigbio-ingestion-derivatives.timer

The timers kick off "oneshot" services that run once and complete and need to be triggered again so the timer handles this.

The get-media service (aka the "fetcher") has its own built-in loop to run continuously.

The idigbio-ingestion-mediaing-updatedb.timer can be set up to run a `daily` job but we currently do not have enough memory to perform indexing on the same machine where this service is running. Therefore we no longer start this service and instead run it manually with every data ingestion.


7. To update the code used by the services, change to the `idigbio-ingestion` user's checkout of this repo and `git pull`.   Then as root or a user with sudo permissions, execute:

    systemctl daemon-reload

and restart the relevant services with 

    systemctl restart <service>

### Docker Image

The [idb-backend](https://hub.docker.com/r/idigbio/idb-backend/) image
is built off of this repository.


## Running

The main entry points are the `idb` and `idigbio-ingestion` commands; you can run them with `--help` to
see what subcommands are available. When invoking this script there is
no need to set the `PYTHONPATH`.

In any invocation an `idigbio.json` must be present in either `$PWD`,
`$HOME`, or `/etc/idigbio`.

For convenience we can create links to the virtual environment entrypoints in `/usr/local/bin/` so we do not need to activate the virtual environment explicitly or use the full path.

```
$ sudo ln -s /home/idigbio-ingestion/idb-backend/venv/bin/idb  /usr/local/bin/idb
$ ls -la /usr/local/bin/idb
lrwxrwxrwx 1 root root 48 Aug 16 20:23 /usr/local/bin/idb -> /home/idigbio-ingestion/idb-backend/venv/bin/idb
$ sudo ln -s /home/idigbio-ingestion/idb-backend/venv/bin/idigbio-ingestion  /usr/local/bin/idigbio-ingestion
$ ls -la /usr/local/bin/idigbio-ingestion
lrwxrwxrwx 1 root root 62 Aug 16 20:24 /usr/local/bin/idigbio-ingestion -> /home/idigbio-ingestion/idb-backend/venv/bin/idigbio-ingestion
```
### Data API

This serves the `api.idigbio.org` interface

    idb run-server

### Celery worker

This package can also be run as a [celery worker]; this is used by the
data api that launches some tasks (most notably download building) via
celery to a background worker.

    celery worker --app=idigbio_workers -l INFO

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

### Running the Test Suite

The test suite can be run by executing `py.test` (or `pytest`).

See the [README.md in the `tests` subdirectory](tests/README.md) for more information.

There are a number of important dependencies noted there.

