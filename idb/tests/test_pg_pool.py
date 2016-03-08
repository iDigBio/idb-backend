import logging

import gevent
import psycopg2
import psycopg2.extensions
import pytest
from psycopg2.extras import RealDictCursor, DictCursor

from idb.postgres_backend.gevent_helpers import GeventedConnPool


log = logging.getLogger("testpgpool")


def pytest_generate_tests(metafunc):
    if 'pool' in metafunc.fixturenames:
        metafunc.parametrize("pool", range(1, 10, 2), indirect=True)


class PassException(Exception):
    pass


@pytest.fixture()
def testdb(scope="module"):
    dbparams = {"host": "localhost", "dbname": "test",
                "user": "test", "password": "test"}
    with psycopg2.connect(**dbparams) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT 1")
    return dbparams


@pytest.fixture()
def pool1(testdb):
    return GeventedConnPool(maxsize=1, **testdb)


@pytest.fixture()
def pool(testdb, request):
    log.debug("Making pool of size %d", request.param)
    p = GeventedConnPool(maxsize=request.param, **testdb)
    request.addfinalizer(gevent.wait)
    return p


def test_basic_conn(pool1):
    result = pool1.fetchone("SELECT 1")
    assert result[0] == 1
    gevent.wait()


def test_exception_rollback(pool1):
    _conn = None
    try:
        with pool1.connection() as conn:
            _conn = conn
            raise PassException()
    except PassException:
        pass
    gevent.wait()
    assert _conn.get_transaction_status() ==  psycopg2.extensions.TRANSACTION_STATUS_IDLE
    assert pool1.pool.qsize() <= pool1.maxsize


def test_repeated_conn(pool):
    count = 20
    spawns = [gevent.spawn(pool.execute, 'select pg_sleep(0.1);')
              for _ in range(0, count)]
    assert len(gevent.wait(spawns, timeout=3)) == count, \
        "Took too long to join"
    gevent.wait()
    assert pool.pool.qsize() <= pool.maxsize


def test_closing_outside_of_block(pool):
    for i in range(0, 10):
        try:
            conn = pool.get()
            conn.close()
            pool.put(conn)
        except:
            log.exception("Error")
    gevent.wait(timeout=5)
    assert pool.pool.qsize() <= pool.maxsize


def test_closing_in_block(pool):
    for i in range(0, 10):
        with pytest.raises(psycopg2.InterfaceError):
            with pool.connection() as conn:
                conn.close()

    gevent.wait(timeout=5)
    assert pool.pool.qsize() <= pool.maxsize


def test_closed_connections(pool1):
    _conn = None
    with pool1.connection() as conn:
        _conn = conn
    gevent.wait()
    _conn.close()
    with pool1.connection() as conn:
        assert _conn is not conn
        assert conn.status == psycopg2.extensions.STATUS_READY
    gevent.wait(timeout=1)
    assert pool1.pool.qsize() <= pool1.maxsize


def test_transaction_isolation_reset(pool1):
    _conn = None
    with pool1.connection(isolation_level=psycopg2.extensions.ISOLATION_LEVEL_SERIALIZABLE) as conn:
        _conn = conn
        conn.cursor().execute("SELECT 1")
    with pool1.connection() as conn:
        assert _conn is conn
        assert conn.isolation_level != psycopg2.extensions.ISOLATION_LEVEL_SERIALIZABLE
    gevent.wait(timeout=1)
    assert pool1.pool.qsize() <= pool1.maxsize


def test_cursor_type(pool1):
    with pool1.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute("Select 1 as foo")
        result = cur.fetchone()
        assert result['foo'] == 1
        assert isinstance(result, dict)

    with pool1.cursor(cursor_factory=DictCursor) as cur:
        cur.execute("Select 1 as foo")
        result = cur.fetchone()
        assert result['foo'] == 1


def test_closeall(pool):
    count = 20
    spawns = [gevent.spawn(pool.execute, 'select 1;')
              for _ in range(0, count)]
    pool.closeall()

    with pytest.raises(psycopg2.pool.PoolError):
        pool.get()
    gevent.wait()


def test_fetchone(pool1):
    result = pool1.fetchone("Select 1 as foo")
    assert result[0] == 1


def test_fetchall(pool1):
    result = pool1.fetchall("Select 1 as foo UNION select 2 as foo")
    assert len(result) == 2
    assert result[0][0] == 1
    assert result[1][0] == 2


def test_fetchiter(pool1):
    gen = pool1.fetchiter("Select 1 as foo UNION select 2 as foo")
    assert next(gen)[0] == 1
    assert next(gen)[0] == 2
    with pytest.raises(StopIteration):
        next(gen)
