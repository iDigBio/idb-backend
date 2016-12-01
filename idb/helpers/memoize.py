from __future__ import division, absolute_import
from __future__ import print_function

from functools import wraps
import cPickle
from idb.helpers.logging import getLogger
from atomicfile import AtomicFile

logger = getLogger('memoize')

def _memoize_0args(fn):
    "A memoizer for a no arg function; only need a single cell storage"
    @wraps(fn)
    def memo():
        if memo.__cache__ is memo:
            memo.__cache__ = fn()
        return memo.__cache__
    memo.__cache__ = memo
    return memo


def _memoize_nargs_error(fn):
    "A memoizer for function w/ arbitrary length arguments."
    memory = {}

    @wraps(fn)
    def memo(*args, **kwargs):
        key = hash((hash(args), hash(tuple(sorted(kwargs.items())))))
        try:
            v = memory[key]
        except KeyError:
            v = memory[key] = fn(*args, **kwargs)
        return v
    memo.__cache__ = memory
    return memo


def _memoize_nargs_callthru(fn):
    "A memoizer for function w/ arbitrary length arguments."
    memory = {}

    @wraps(fn)
    def memo(*args, **kwargs):
        try:
            key = hash((hash(args), hash(tuple(sorted(kwargs.items())))))
        except TypeError:  # args were unhashable
            return fn(*args, **kwargs)
        try:
            v = memory[key]
        except KeyError:
            v = memory[key] = fn(*args, **kwargs)
        return v
    memo.__cache__ = memory
    return memo


def _memoize_nargs_cPickle(fn):
    "A memoizer for function w/ arbitrary length arguments."
    memory = {}
    import cPickle

    @wraps(fn)
    def memo(*args, **kwargs):
        key = (cPickle.dumps(args, cPickle.HIGHEST_PROTOCOL),
               cPickle.dumps(kwargs, cPickle.HIGHEST_PROTOCOL))
        try:
            v = memory[key]
        except KeyError:
            v = memory[key] = fn(*args, **kwargs)
        return v
    memo.__cache__ = memory
    return memo


def memoized(unhashable="cPickle"):
    "Decorator to memoize a function."
    def getfn(fn):
        if fn.func_code.co_argcount == 0:
            memo = _memoize_0args(fn)
        elif unhashable == "call":
            memo = _memoize_nargs_callthru(fn)
        elif unhashable == "cPickle":
            memo = _memoize_nargs_cPickle(fn)
        elif unhashable == "error":
            memo = _memoize_nargs_error(fn)
        else:
            raise ValueError(
                "Uknown unhashable memoization strategy, expected {call, cPickle, error}")
        if memo.__doc__:
            memo.__doc__ = memo.__doc__ + "\n\nThis function is memoized."
        else:
            memo.__doc__
        return memo
    return getfn


def filecached(filename, writeback=True):
    "Cache (with pickle) the results of wrapped fn in a file"
    def writecache(value):
        logger.debug("Writing cache to %r", filename)
        with AtomicFile(filename, 'wb') as f:
            cPickle.dump(value, f)

    def getfn(fn):
        @wraps(fn)
        def thunk(*args, **kwargs):
            val = None
            try:
                with open(filename, 'rb') as f:
                    val = cPickle.load(f)
                    logger.debug("read cache from %r", filename)
            except (IOError, EOFError):
                val = fn(*args, **kwargs)
                writecache(val)
            if writeback:
                import atexit
                atexit.register(writecache, val)
            return val
        return thunk
    return getfn
