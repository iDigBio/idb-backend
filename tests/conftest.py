from __future__ import division, absolute_import, print_function
"""Setup a lot of fixtures for testing idb_flask_authn

Notably this can create a test idigbio database. In order for this to
work you need a local postgres instance with the user/pass test/test
that can create databases.


"""

import gevent
import gevent.monkey
import pytest
import psycopg2
from py.path import local


def pytest_addoption(parser):
    parser.addoption("--gmp", action="store_true", default=False,
                     help="Run gevent.monkey.patch_all()")


@pytest.fixture(scope="session", autouse=True)
def logger():
    "Setup test logging, provide a logger to use in tests"
    from idb.helpers.logging import idblogger
    return idblogger.getChild('tests')


@pytest.fixture(scope="session", autouse=True)
def gmp(request, logger):
    if request.config.getoption("--gmp"):
        logger.info("Monkeypatching")
        gevent.monkey.patch_all()


@pytest.fixture()
def prodenv(logger):
    from idb import config
    config.ENV = 'prod'
    logger.info("Env: %s", config.ENV)


@pytest.fixture()
def testenv(logger):
    from idb import config
    config.ENV = 'test'
    logger.info("Env: %s", config.ENV)


@pytest.fixture()
def pngpath():
    p = local(__file__).dirpath('data/idigbio_logo.png')
    assert p.exists()
    return p


@pytest.fixture()
def jpgpath():
    p = local(__file__).dirpath('data/idigbio_logo.jpg')
    assert p.exists()
    return p


@pytest.fixture()
def zippath():
    p = local(__file__).dirpath('data/test.zip')
    assert p.exists()
    return p


@pytest.fixture()
def mp3path():
    p = local(__file__).dirpath('data/whip-poor-will.mp3')
    assert p.exists()
    return p


@pytest.fixture()
def schemapath():
    p = local(__file__).dirpath('data/schema.sql')
    assert p.exists()
    return p


@pytest.fixture()
def testdatapath():
    p = local(__file__).dirpath('data/testdata.sql')
    assert p.exists()
    return p


@pytest.fixture(scope="session")
def testdb(logger):
    "Provide the connection spec for a local test db; ensure it works"
    from idb.postgres_backend import DEFAULT_OPTS
    spec = DEFAULT_OPTS.copy()
    spec['dbname'] = 'test_idigbio'
    spec['host'] = 'localhost'
    spec['user'] = 'test'
    spec['password'] = 'test'
    logger.info("Verifying testdb %r", spec)
    with psycopg2.connect(**spec) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT 1")
    return spec


@pytest.fixture(scope="session")
def testdbpool(request, testdb, logger):
    "a DB pool to the test database"
    from idb.postgres_backend.gevent_helpers import GeventedConnPool
    logger.debug("Creating ")
    dbpool = GeventedConnPool(**testdb)

    def cleanup():
        logger.info("Cleanup testdbpool")
        dbpool.closeall()
    request.addfinalizer(cleanup)
    return dbpool


@pytest.fixture()
def testschema(schemapath, testdbpool, logger):
    "Ensure a fresh version of the idb schema, with no data loaded"
    logger.info("Loading schema into testdb")
    testdbpool.execute(schemapath.open('r', encoding='utf-8').read())


@pytest.fixture()
def testdata(testenv, testschema, testdbpool, testdatapath, logger):
    "Ensure the standard set of testdata is loaded, nothing more"
    logger.info("Loading data into testdb")
    testdbpool.execute(testdatapath.open('r', encoding='utf-8').read())


@pytest.fixture()
def testidbmodel(request, testdbpool, testschema, logger):
    from idb.postgres_backend.db import PostgresDB
    i = PostgresDB(pool=testdbpool)

    def cleanup():
        logger.info("Cleanup testidbmodel")
        i.rollback()
        i.close()
        gevent.wait()
    request.addfinalizer(cleanup)
    return i
