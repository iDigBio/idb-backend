from __future__ import absolute_import
import contextlib
import uuid
import sys

import psycopg2.extensions
import psycopg2.pool

import gevent
import gevent.lock
from gevent.queue import Queue
from gevent.socket import wait_read, wait_write

from idb.helpers.logging import idblogger

log = idblogger.getChild('gevent_helpers')


def gevent_wait_callback(conn, timeout=None):
    """A wait callback useful to allow gevent to work with Psycopg."""
    while 1:
        state = conn.poll()
        if state == psycopg2.extensions.POLL_OK:
            break
        elif state == psycopg2.extensions.POLL_READ:
            wait_read(conn.fileno(), timeout=timeout)
        elif state == psycopg2.extensions.POLL_WRITE:
            wait_write(conn.fileno(), timeout=timeout)
        else:
            raise psycopg2.OperationalError(
                "Bad result from poll: %r" % state)

psycopg2.extensions.set_wait_callback(gevent_wait_callback)


# this is mostly based on
# https://github.com/gevent/gevent/blob/master/examples/psycopg2_pool.py
# with some connection state checks from
# https://github.com/dvarrazzo/psycopg/blob/2_4_5/lib/pool.py
#

class GeventedConnPool(object):
    closed = False
    maxsize = 0
    pool = None
    _connectargs = None

    def __init__(self, maxsize=10, **connectargs):
        self.maxsize = maxsize
        self.pool = Queue()
        self.lock = gevent.lock.BoundedSemaphore(maxsize)
        self._connectargs = connectargs

    def _connect(self):
        return psycopg2.connect(**self._connectargs)

    def _reset_and_return(self, conn):
        try:
            conn.reset()
            self.pool.put(conn)
        except:
            gevent.get_hub().handle_error(conn, *sys.exc_info())
        finally:
            self.lock.release()

    def get(self):
        if self.closed:
            raise psycopg2.pool.PoolError("connection pool is closed")
        self.lock.acquire()
        try:
            conn = self.pool.get_nowait()
            if conn.closed or conn.status != psycopg2.extensions.STATUS_READY:
                log.info("Conn isn't ready: %r", conn.status)
                conn.close()
                self.lock.release()
                return self.get()
            return conn
        except gevent.queue.Empty:
            try:
                return self._connect()
            except:
                self.lock.release()
                raise

    def put(self, conn):
        assert conn is not None
        try:
            if self.closed:
                conn.close()
            if conn.closed:
                # If the connection is closed, we just discard it.
                self.lock.release()
                return

            # Return the connection into a consistent state before putting
            # it back into the pool
            status = conn.get_transaction_status()
            if status == psycopg2.extensions.TRANSACTION_STATUS_UNKNOWN:
                # server connection lost
                conn.close()
                self.lock.release()
                return
            elif status != psycopg2.extensions.TRANSACTION_STATUS_IDLE:
                # connection in error or in transaction
                conn.rollback()
            gevent.spawn(self._reset_and_return, conn)
        except:
            self.lock.release()
            gevent.get_hub().handle_error(conn, *sys.exc_info())

    def closeall(self):
        log.info("Closing all connections")

        while not self.pool.empty():
            conn = self.pool.get_nowait()
            try:
                conn.close()
                self.lock.release()
            except Exception:
                pass
        self.closed = True
        gevent.wait()
        self.closed = False

    @contextlib.contextmanager
    def connection(self, isolation_level=None, autocommit=None):
        conn = self.get()
        try:
            if isolation_level is not None and isolation_level != conn.isolation_level:
                conn.set_isolation_level(isolation_level)
            if autocommit is not None:
                conn.autocommit = autocommit
            yield conn
            conn.commit()
        finally:
            if conn:
                #self.put(conn)
                gevent.spawn(self.put, conn)

    @contextlib.contextmanager
    def cursor(self, *args, **kwargs):
        connargs = {
            'isolation_level': kwargs.pop('isolation_level', None),
            'autocommit':kwargs.pop('autocommit', None)
        }

        if kwargs.pop('named', False) is True:
            kwargs['name'] = str(uuid.uuid4())
        with self.connection(**connargs) as conn:
            yield conn.cursor(*args, **kwargs)


    # Some shortcut functions
    def execute(self, *args, **kwargs):
        """like cursor.execute

        kwargs to cursor, positional args to execute
        """
        with self.cursor(**kwargs) as cursor:
            cursor.execute(*args)
            return cursor.rowcount

    def executemany(self, *args, **kwargs):
        """Pasthrough to cursor.executemany

        kwargs to cursor, positional args to executemany"""
        with self.cursor(**kwargs) as cursor:
            cursor.executemany(*args)
            return cursor.rowcount

    def fetchone(self, *args, **kwargs):
        """like cursor.fetchone

        kwargs to cursor, positional args to execute
        """
        with self.cursor(**kwargs) as cursor:
            cursor.execute(*args)
            return cursor.fetchone()

    def fetchall(self, *args, **kwargs):
        """like cursor.fetchall

        kwargs to cursor, positional args to execute
        """
        with self.cursor(**kwargs) as cursor:
            cursor.execute(*args)
            return cursor.fetchall()

    def fetchiter(self, *args, **kwargs):
        """iterate over a cursors results

        kwargs to cursor, positional args to execute
        """
        with self.cursor(**kwargs) as cursor:
            cursor.execute(*args)
            for f in cursor:
                yield f
