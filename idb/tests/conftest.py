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


@pytest.fixture
def app():
    from idb.data_api.api import app
    return app


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
def testdb(request, schemapath, logger):
    dbname = 'test_idigbio'
    template1 = {
        "host": "localhost",
        "user": "test",
        "password": "test",
        "dbname": "template1"
    }
    testdbspec = template1.copy()
    testdbspec['dbname'] = dbname

    with psycopg2.connect(**template1) as conn:
        conn.autocommit = True
        with conn.cursor() as cur:
            cur.execute('DROP DATABASE IF EXISTS "{0}"'.format(dbname))
            cur.execute('CREATE DATABASE "{0}"'.format(dbname))
    with psycopg2.connect(**testdbspec) as conn:
        conn.autocommit = True
        with conn.cursor() as cur:
            cur.execute(schemapath.open('r', encoding='utf-8').read())

    def cleanup():
        logger.info("Cleanup testdb")
        gevent.wait()
        with psycopg2.connect(**template1) as conn:
            conn.autocommit = True
            with conn.cursor() as cur:
                cur.execute('DROP DATABASE "{0}"'.format(dbname))
    request.addfinalizer(cleanup)
    return testdbspec


@pytest.fixture()
def testdbpool(request, testdb, logger):
    from idb.postgres_backend.gevent_helpers import GeventedConnPool
    dbpool = GeventedConnPool(**testdb)

    def cleanup():
        logger.info("Cleanup testdbpool")
        dbpool.closeall()
        gevent.wait()
    request.addfinalizer(cleanup)
    return dbpool


@pytest.fixture()
def testidbmodel(request, testdbpool, logger):
    from idb.postgres_backend.db import PostgresDB
    i = PostgresDB(pool=testdbpool)

    def cleanup():
        logger.info("Cleanup testidbmodel")
        i.rollback()
        i.close()
        gevent.wait()
    request.addfinalizer(cleanup)
    return i
