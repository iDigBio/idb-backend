# idb-backend

[![Build Status](https://travis-ci.com/iDigBio/idb-backend.svg?branch=master)](https://travis-ci.com/iDigBio/idb-backend)

iDigBio server and backend code for data ingestion and data API.

## Installation

### System Dependencies

#### Python 3 instructions:

In Ubuntu 18.04:

    apt install python3-dev libblas-dev liblapack-dev \
      libatlas-base-dev gfortran libgdal-dev libpq-dev libgeos-c1v5 \
      libsystemd-dev

For Ingestion and Development, the following are also needed:

In 18.04:

    apt install libxml2 libxslt1-dev ffmpeg fonts-dejavu-core libfreetype6-dev python-systemd

#### Python 2.7 instructions:

This version of idb-backend will not work under Python 2.


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

    celery worker --app=idigbio_workers -l WARNING

[celery worker]: http://docs.celeryproject.org/en/latest/userguide/workers.html

## Development and Testing

You probably want to run in a virtual environment.   You may wish to disable the pip cache
to verify package builds are working properly.


### Python 3

```bash

$ python --version
Python 2.7.17

$ python3 -m virtualenv -p python3 .venv
Already using interpreter /usr/bin/python3
Using base prefix '/usr'
New python executable in /tmp/venvtest/.venv/bin/python3
Also creating executable in /tmp/venvtest/.venv/bin/python
Installing setuptools, pkg_resources, pip, wheel...done.

$ source .venv/bin/activate

$ python --version
Python 3.6.9

$ pip --no-cache-dir install -e .

$ pip --no-cache-dir install -r requirements.txt
```

### Python 2 (obsolete)

This project is no longer compatible with Python 2.


### Develop in Container

It is possible in the future that this project will be runnable using "Open in container" features of Microsoft Visual Studio Code (aka vscode or just `code`).


### Running the Test Suite

The test suite can be run by executing `py.test` (or `pytest`).

See the [README.md in the `tests` subdirectory](tests/README.md) for more information.

There are a number of important dependencies noted there.


