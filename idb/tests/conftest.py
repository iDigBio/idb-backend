"""Setup a lot of fixtures for testing idb_flask_authn

Notably this can create a test idigbio database. In order for this to
work you need a local postgres instance with the user/pass test/test
that can create databases.


"""
import logging
import gevent
import pytest
import psycopg2
from py.path import local

log = logging.getLogger('idb.tests')

@pytest.fixture(scope="session", autouse=True)
def logger():
    logging.root.setLevel(logging.DEBUG)
    return logging.getLogger('idb.tests')


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
def idbmodel(request):
    from idb.postgres_backend.db import PostgresDB
    i = PostgresDB()

    def cleanup():
        log.info("Cleanup idbmodel")
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
def testdb(request, schemapath):
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
        log.info("Cleanup testdb")
        gevent.wait()
        with psycopg2.connect(**template1) as conn:
            conn.autocommit = True
            with conn.cursor() as cur:
                cur.execute('DROP DATABASE "{0}"'.format(dbname))
    request.addfinalizer(cleanup)
    return testdbspec


@pytest.fixture()
def testdbpool(request, testdb):
    from idb.postgres_backend.gevent_helpers import GeventedConnPool
    dbpool = GeventedConnPool(**testdb)

    def cleanup():
        log.info("Cleanup testdbpool")
        dbpool.closeall()
        gevent.wait()
    request.addfinalizer(cleanup)
    return dbpool


@pytest.fixture()
def testidbmodel(request, testdbpool):
    from idb.postgres_backend.db import PostgresDB
    i = PostgresDB(pool=testdbpool)

    def cleanup():
        log.info("Cleanup testidbmodel")
        i.rollback()
        i.close()
        gevent.wait()
    request.addfinalizer(cleanup)
    return i
