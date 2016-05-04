# idb-backend

This is the collection of code that makes up the iDigBio server side.

## Installation

### System Dependencies

Currently this project only works in python2.7.

The following library packages will need to be installed to run the api (assuming
Ubuntu 14.04):

    apt-get install python2.7-dev libgeos-c1 libblas-dev liblapack-dev \
      libatlas-base-dev gfortran libgdal-dev`

For ingestion the following are also needed:

    apt-get install libfontconfig-dev libav-tools libxml2 libxslt1-dev

### Python Dependencies

Install all of the python dependencies, this will be very slow the
first time, after that pip caches help.

    pip install -r requirements.txt

For ingestion you will also need to run

    pip install -r idigbio_ingestion/requirements.txt

For testing you will also need to run

    pip install -r test-requirements.txt

### Package installation

This package itself needs to be installed to generate the CLI
scripts. To install it editably (so that `git pull` continues to
updates everything):

    pip install -e .

## Running

The main entry point is the `idb` command; you can run `idb --help` to
see what subcommands are available. When invoking this script there is
no need to set the `PYTHONPATH`.
