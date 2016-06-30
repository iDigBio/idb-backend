"""Some helper functions for dealing with signals"""

from __future__ import division, absolute_import
from __future__ import print_function

from datetime import datetime
from contextlib import contextmanager

from signal import (  # noqa
    SIG_DFL, SIG_IGN, SIGABRT, SIGHUP,
    SIGINT, SIGQUIT, SIGUSR1, SIGUSR2, SIGTERM)
from signal import signal as _signal


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
def signalcm(signalnum, handler, call_original=True):
    """Install a new signal handler

    The original signal handler will be restored at the end.

    If ``call_original`` is True (DEFAULT) and the original signal
    handler is callable, it will invoked after the specified handler.

    """
    previous_handler = None

    def wrapper(signalnum, frame):
        handler(signalnum, frame)
        if callable(previous_handler):
            previous_handler(signalnum, frame)

    previous_handler = _signal(signalnum, wrapper if call_original else handler)
    try:
        yield
    finally:
        _signal(signalnum, previous_handler)


@contextmanager
def doubleinterrupt(callback=None, timeout=10):
    """Force two interrupts to pass through

    This puts in a handler for SIGINT that will just log and ignore
    them unless there are two SIGINTs within the specified timeout
    (DEFAULT: 10)

    if a callback (0-arg) is provided it is invoked every SIGINT.

    """
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

    previous_handler = _signal(SIGINT, inthandler)
    try:
        yield
    finally:
        _signal(SIGINT, previous_handler)
