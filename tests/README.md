# Tests

## Layout

The current convention for idb-backend is that tests will live
in a separate directory tree but will mimic the codebase structure.

For example...


The code:

```idb-backend/idigbio_ingestion/mediaing/derivatives.py```


The tests:

```idb-backend/tests/idigbio_ingestion/mediaing/test_derivatives.py```


Run the project tests with typical pytest execution:


```
$ py.test 
================================================================================================ test session starts ================================================================================================
platform linux2 -- Python 2.7.6, pytest-3.0.7, py-1.4.32, pluggy-0.4.0
rootdir: /home/dstoner/git/idb-backend, inifile: setup.cfg
plugins: mock-1.5.0, flask-0.10.0, cov-2.4.0, catchlog-1.2.2, celery-4.0.2
collected 199 items 

tests/idb/test_cli.py ...
tests/idb/test_data_api_basic.py ssss
tests/idb/test_data_api_downloads.py sssssssss
tests/idb/test_data_api_lookups.py sssss
tests/idb/test_data_api_media.py ssssssssssssssssssssssssss
tests/idb/test_helpers_conversions.py ................................
tests/idb/test_helpers_idb_flask_authn.py s
tests/idb/test_helpers_media_validation.py ........
tests/idb/test_helpers_memoize.py .......
tests/idb/test_helpers_query_shim.py ...........................
tests/idb/test_helpers_storage.py ...
tests/idb/test_idbmodel.py s
tests/idb/test_indexer_indexing.py .
tests/idb/test_logging.py .......
tests/idb/test_media_object.py ....sssssss
tests/idb/test_pg_pool.py sssssssssssssssssssssssssssss
tests/idigbio_ingestion/mediaing/test_derivatives.py ...........
tests/idigbio_ingestion/mediaing/test_fetcher.py .....
tests/idigbio_ingestion/mediaing/test_mediaing.py .
tests/idigbio_workers/lib/test_download.py ........

====================================================================================== 117 passed, 82 skipped in 97.20 seconds ======================================================================================
```


To run a smaller subset of tests: 

```
$ pytest tests/idigbio_ingestion
================================================================================================ test session starts ================================================================================================
platform linux2 -- Python 2.7.6, pytest-3.0.7, py-1.4.32, pluggy-0.4.0
rootdir: /home/dstoner/git/idb-backend, inifile: setup.cfg
plugins: mock-1.5.0, flask-0.10.0, cov-2.4.0, catchlog-1.2.2, celery-4.0.2
collected 17 items 

tests/idigbio_ingestion/mediaing/test_derivatives.py ...........
tests/idigbio_ingestion/mediaing/test_fetcher.py .....
tests/idigbio_ingestion/mediaing/test_mediaing.py .

============================================================================================= 17 passed in 1.92 seconds =============================================================================================

```

To run an individual test matching a test name:

```
$ pytest -v -k test_pngpath_is_usable
======================================================================= test session starts ========================================================================
platform linux2 -- Python 2.7.6, pytest-3.0.7, py-1.4.32, pluggy-0.4.0 -- /home/dstoner/git/Envs/idb-backend/bin/python
cachedir: ../.cache
rootdir: /home/dstoner/git/idb-backend, inifile: setup.cfg
plugins: mock-1.5.0, flask-0.10.0, cov-2.4.0, catchlog-1.2.2, celery-4.0.2
collected 206 items 

test_data_exists.py::test_pngpath_is_usable PASSED

======================================================================= 205 tests deselected =======================================================================
============================================================= 1 passed, 205 deselected in 0.33 seconds =============================================================

```

To exclude a single set of tests that are failing (or Seg Faulting!), add the `--deselect` option to the pytest command:

    py.test --deselect=tests/idigbio_ingestion/mediaing/test_derivatives.py

To find out why tests are being Skipped, add the `-rxs` options.


More on pytest at https://docs.pytest.org/en/latest/usage.html


## Dependencies

Some idb-backend tests depend on external resources, such as a local test postgres database or the production Elasticsearch cluster.

* Database tests will be SKIPPED if the local postgres test database is not available.

* Tests that depend on Elasticsearch will FAIL if the Elasticsearch cluster cannot be reached (fail very slowly in fact), or if there is some other failure.

The local postgresql 9.5 DB is named `test_idigbio` with user/pass `test` / `test`.

Note: The data in the db with that name will be destroyed during testing.

A temporary instance of postgres running in docker will suffice:

```
$ docker run --rm --name postgres_test_idigbio --network host -e POSTGRES_PASSWORD=test -e POSTGRES_USER=test -e POSTGRES_DB=test_idigbio -d postgres:9.5
```


## Running All Tests

Due to the dependencies mentioned above, you may wish to run the database in docker each time.  The sleep is needed to allow postgres time to start accepting connections.

    docker run --rm --name postgres_test_idigbio --network host \
      -e POSTGRES_PASSWORD=test -e POSTGRES_USER=test -e POSTGRES_DB=test_idigbio  \
      -d postgres:9.5 && \
      sleep 5; \
      py.test ; \
      docker stop postgres_test_idigbio
        


## Local database

The recommended approach is to run postgres via docker (see above).

### Create a local postgres DB

However, if you have a full installation of postgres running locally that you wish to use, the db can be manually created with: 

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
