"""Setup a lot of fixtures for testing idb_flask_authn

Notably this can create a test idigbio database. In order for this to
work you need a local postgres instance with the user/pass test/test
that can create databases.


"""

import gevent
import pytest
import psycopg2
from py.path import local


@pytest.fixture(scope="session", autouse=True)
def logger():
    from idb.helpers.logging import idblogger, configure_app_log
    configure_app_log(verbose=2)
    from idb import config
    idblogger.debug("Test Env: %s", config.ENV)
    return idblogger.getChild('tests')


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
def idbmodel(request, logger):
    from idb.postgres_backend.db import PostgresDB
    i = PostgresDB()

    def cleanup():
        logger.info("Cleanup idbmodel")
        i.rollback()
        i.close()
    request.addfinalizer(cleanup)
    return i


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
    from idb.postgres_backend import pg_conf
    spec = pg_conf.copy()
    spec['database'] = spec['dbname'] = 'test_idigbio'
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
def testdata(testschema, testdbpool, testdatapath, logger):
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


@pytest.fixture
def app(testdbpool, testdata, logger):
    from idb.data_api import api
    reload(api)
    app = api.app
    app.config['DB'] = testdbpool

    def cleanup(exception):
        logger.info("Cleanup app idbmodel")
        from idb.data_api.common import idbmodel
        idbmodel.rollback()
        idbmodel.close()
        gevent.wait()
    app.teardown_appcontext(cleanup)

    return app
