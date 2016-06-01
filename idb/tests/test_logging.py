import pytest
from logging import DEBUG, INFO, WARNING, ERROR  # noqa: ignore=F401
from idb.helpers import logging

@pytest.fixture()
def ecaplog(caplog):
    """Return a caplog where all the records have been emptied

    This helps solve that other fixtures log some setup information,
    but is probably racy about this fixture being evaluated last.

    """
    a = caplog.records()
    while len(a):
        a.pop()
    return caplog

def test_fntimed_basic(ecaplog):
    @logging.fntimed()
    def foo(): pass

    foo()
    assert len(ecaplog.records()) == 1
    rec = ecaplog.records()[0]
    assert rec.levelname == 'DEBUG'
    assert rec.msg.endswith('s')

    @logging.fntimed
    def bar(): pass

    bar()
    assert len(ecaplog.records()) == 2
    rec = ecaplog.records()[-1]
    assert rec.levelname == 'DEBUG'
    assert rec.msg.endswith('s')


def test_fntimed_noop():
    @logging.fntimed(log=None)
    def foo(): pass
    foo()


def test_fntimed_speciallevel(ecaplog):
    @logging.fntimed(log=logging.idblogger.info)
    def foo(): pass
    foo()
    assert len(ecaplog.records()) == 1
    rec = ecaplog.records()[0]
    assert rec.levelname == 'INFO'
    assert rec.msg.endswith('s')

def test_fntimed_class(ecaplog):
    class Foo(object):
        @logging.fntimed()
        def bar(self): pass

    Foo().bar()
    assert len(ecaplog.records()) == 1
    rec = ecaplog.records()[0]
    assert rec.levelname == 'DEBUG'
    assert rec.msg.endswith('s')


def test_fnlogged_reraise(ecaplog):
    @logging.fnlogged()
    def foo():
        raise Exception('woot')
    with pytest.raises(Exception):
        foo()

    assert len(ecaplog.records()) == 1
    rec = ecaplog.records()[0]
    assert rec.levelname == 'ERROR'

    @logging.fnlogged(reraise=False)
    def bar():
        raise Exception('woot')
    bar()

    assert len(ecaplog.records()) == 2
    rec = ecaplog.records()[1]
    assert rec.levelname == 'ERROR'


def test_fnlogged_timed(ecaplog):
    @logging.fnlogged(logger=logging.idblogger, time_level=WARNING)
    def foo(): pass
    foo()
    assert len(ecaplog.records()) == 1
    rec = ecaplog.records()[0]
    assert rec.levelname == 'WARNING'
    assert rec.msg.endswith('s')


def test_fnlogged_noop(ecaplog):
    @logging.fnlogged(logger=False)
    def foo():
        raise Exception()
    with pytest.raises(Exception):
        foo()
    assert len(ecaplog.records()) == 0
