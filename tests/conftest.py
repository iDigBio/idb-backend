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
from functools import partial
from py.path import local


def pytest_addoption(parser):
    parser.addoption("--gmp", action="store_true", default=True,
                     help="Run gevent.monkey.patch_all()")
    parser.addoption("--pguser", action="store", default="test")
    parser.addoption("--pgpass", action="store", default="test")



@pytest.fixture(scope="session", autouse=True)
def logger():
    "Setup test logging, provide a logger to use in tests"
    from idb.helpers.logging import idblogger, configure_app_log
    configure_app_log(2)
    return idblogger.getChild('tests')


@pytest.fixture(scope="session", autouse=False)
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
def emlpathdir():
    p = local(__file__).dirpath('data/eml/')
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
def testdb(request, logger):
    "Provide the connection spec for a local test db; ensure it works"
    from idb.postgres_backend import DEFAULT_OPTS
    spec = DEFAULT_OPTS.copy()
    spec['dbname'] = 'test_idigbio'
    spec['host'] = 'localhost'
    spec['user'] = request.config.getoption("--pguser")
    spec['password'] = request.config.getoption("--pgpass")
    logger.info("Verifying testdb %r", spec)
    try:
        with psycopg2.connect(**spec) as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT 1")
    except psycopg2.OperationalError:
        pytest.skip("Unable to connect to PG.")

    return spec


@pytest.fixture()
def testdbpool(request, testdb, logger):
    "A DB pool to the test database"
    from idb.postgres_backend.gevent_helpers import GeventedConnPool
    logger.debug("Creating ConnPool: %r", testdb)
    dbpool = GeventedConnPool(**testdb)

    def ro_setter(val):
        logger.debug("Setting DB to READ ONLY=%s", val)
        dbpool.execute(
            "ALTER DATABASE {0} SET default_transaction_read_only = {1};".format(testdb['dbname'], val),
            readonly=False)
        dbpool.closeall()
    if 'readonly' in request.keywords:
        ro_setter("true")
    else:
        ro_setter("false")

    def cleanup():
        ro_setter("false")
        logger.info("Cleanup dbpool")
        dbpool.closeall()
    request.addfinalizer(cleanup)
    return dbpool


@pytest.fixture()
def testschema(schemapath, testdbpool, logger):
    "Ensure a fresh version of the idb schema, with no data loaded"
    logger.info("Loading schema into testdb")
    testdbpool.execute(schemapath.open('r', encoding='utf-8').read(), readonly=False)


@pytest.fixture()
def testdata(testenv, testschema, testdbpool, testdatapath, logger):
    "Ensure the standard set of testdata is loaded, nothing more"
    logger.info("Loading data into testdb")
    testdbpool.execute(testdatapath.open('r', encoding='utf-8').read(), readonly=False)


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
