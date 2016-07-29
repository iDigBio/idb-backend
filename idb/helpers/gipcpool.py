"""This is a multiprocessing like pool using gipc and gevent

A *major* distinction though is that we don't attempt to get any
result object back, just the processes return code.

"""

from __future__ import division, absolute_import, print_function

import functools
import multiprocessing

import gevent.pool
import gipc
import greenlet

from idb.helpers.logging import idblogger

logger = idblogger.getChild('gipc')

def spawn(target, *args, **kwargs):
    daemon = kwargs.pop('daemon', False)
    try:
        p = gipc.start_process(target, args, kwargs, daemon=daemon)
        p.join()
        return p.exitcode
    except (KeyboardInterrupt, greenlet.GreenletExit) as e:
        logger.debug("Killing child proc %s on %r", p, e)
        p.terminate()
        p.join()
        raise
    except:
        logger.exception("Failed on child %s", p)
        raise


class Pool(object):

    def __init__(self, size=None):
        if size is None:
            size = multiprocessing.cpu_count()
        self.gpool = gevent.pool.Pool(size)

    def imap_unordered(self, func, *iterables, **kwargs):
        "Based on gevent.imap's interface"
        pimap = self.gpool.imap_unordered(functools.partial(spawn, func), *iterables, **kwargs)
        try:
            for i in pimap:
                yield i
        except (KeyboardInterrupt, greenlet.GreenletExit) as e:
            logger.debug("Interrupting imap_unordered for %r", e)
            pimap.kill()
            self.kill()
            raise

    def kill(self, *args, **kwargs):
        return self.gpool.kill(*args, **kwargs)
