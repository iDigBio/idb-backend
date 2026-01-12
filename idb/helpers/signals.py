"""Some helper functions for dealing with signals"""

from __future__ import division, absolute_import
from __future__ import print_function

from datetime import datetime
from contextlib import contextmanager

import signal as _signal
from signal import (  # noqa
    SIG_DFL, SIG_IGN, SIGABRT, SIGHUP,
    SIGINT, SIGQUIT, SIGUSR1, SIGUSR2, SIGTERM
)

import threading

from idb.helpers.logging import idblogger

logger = idblogger.getChild('sig')


@contextmanager
def ignored(signalnum):
    """Ignore the specified signal for the duration of this contextmanager

    The original signal handler will be restored at the end.
    """
    with signalcm(signalnum, SIG_IGN, call_original=False):
        yield


@contextmanager
def signalcm(signalnum, handler, call_original=False):
    # Python only allows installing signal handlers in the main thread.
    if threading.current_thread() is not threading.main_thread():
        yield
        return

    previous_handler = _signal.getsignal(signalnum)

    def wrapper(signum, frame):
        handler(signum, frame)
        if call_original and callable(previous_handler):
            previous_handler(signum, frame)

    _signal.signal(signalnum, wrapper if call_original else handler)
    try:
        yield
    finally:
        _signal.signal(signalnum, previous_handler)


@contextmanager
def doubleinterrupt(callback=None, timeout=10):
    """Force two interrupts to pass through"""
    last_interrupted = [datetime.min]

    previous_handler = None

    def inthandler(signum, frame):
        if callback:
            callback()
        delta = (datetime.now() - last_interrupted[0]).total_seconds()
        last_interrupted[0] = datetime.now()
        if delta < timeout:
            if callable(previous_handler):
                previous_handler(signum, frame)
            else:
                raise KeyboardInterrupt()
        else:
            logger.warning(
                "SIGINT swallowed, interrupt again within %ss to confirm",
                timeout)

    previous_handler = _signal.signal(SIGINT, inthandler)
    try:
        yield
    finally:
        _signal.signal(SIGINT, previous_handler)
