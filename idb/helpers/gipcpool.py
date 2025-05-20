# -*- coding: utf-8 -*-
"""
Minimal replacement for `gipcpool.Pool` that uses PyPy stackless
tasklets + channels.  One tasklet per “worker”.
"""

from __future__ import absolute_import, division, print_function

import multiprocessing
import traceback
from stackless import channel, tasklet, run as schedule_run

from idb.helpers.logging import idblogger
logger = idblogger.getChild("continuelet")

# ----------------------------------------------------------------------
# Worker tasklet
# ----------------------------------------------------------------------
def _worker(chan_in, chan_out):
    """
    Receives (func, args, kwargs) tuples on chan_in,
    returns (ok, result) on chan_out.
    """
    while True:
        msg = chan_in.receive()
        if msg is None:          # sentinel → exit
            break

        func, args, kwargs = msg
        try:
            func(*args, **kwargs)
            chan_out.send((True, 0))      # replicate “exit-code 0”
        except Exception:
            logger.error("tasklet error:\n%s", traceback.format_exc())
            chan_out.send((False, 1))


class Pool(object):
    """
    Drop-in replacement for gipc/​gevent pool but based on stackless.
    """

    def __init__(self, size=None):
        self.size = size or max(1, multiprocessing.cpu_count() - 1)

        # Tasklet communication channels
        self._in  = channel()
        self._out = channel()

        # Spawn N workers
        self._workers = [
            tasklet(_worker)(self._in, self._out) for _ in range(self.size)
        ]

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------
    def imap_unordered(self, func, *iterables):
        """
        Yields (ok, exitcode) tuples in arbitrary order,
        just like the old gipcpool variant.
        """
        # Fan-out work: zip longest like itertools.imap
        sentinel = object()
        for args in zip(*[iterables] if len(iterables) == 1 else iterables):
            self._in.send((func, (args[0],) if len(args) == 1 else args, {}))

        # Tell all workers we’re done
        for _ in range(self.size):
            self._in.send(None)

        # Run the scheduler until all tasklets complete
        schedule_run()

        # Drain results
        while not self._out.empty():
            yield self._out.receive()

    def kill(self):
        """
        Immediately stop all workers.
        """
        for _ in range(self.size):
            self._in.send(None)
        schedule_run()
