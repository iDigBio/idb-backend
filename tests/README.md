## tests

The current convention for idb-backend seems to be that tests will live
in a directory structure that mimics the codebase.

For example...


The code:

  idb-backend/idigbio_ingestion/mediaing/derivatives.py  


The tests:

  idb-backend/tests/idigbio_ingestion/mediaing/test_derivatives.py


Run with typical pytest execution:


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
