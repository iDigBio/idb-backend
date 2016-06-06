import pytest

from idb.helpers.memoize import memoized


def test_noarg():
    callcount = [0]

    @memoized()
    def foo():
        callcount[0] += 1
        return callcount[0]

    assert foo() == 1
    assert foo() == 1
    assert foo() == 1


@pytest.mark.parametrize('strat', ['error', 'cPickle', 'call'])
def test_hashable_arg(strat):
    callcount = [0]

    @memoized(strat)
    def foo(a, b=1):
        callcount[0] += 1
        return a+b

    assert foo(0, 0) == 0
    assert callcount[0] == 1
    assert foo(0, 0) == 0
    assert callcount[0] == 1

    assert foo(1) == 2
    assert callcount[0] == 2
    assert foo(1) == 2
    assert callcount[0] == 2

    assert foo(2) == 3
    assert callcount[0] == 3
    assert foo(2) == 3
    assert callcount[0] == 3

    assert foo(2, b=2) == 4
    assert callcount[0] == 4
    assert foo(2, b=2) == 4
    assert callcount[0] == 4


def test_unhashable_arg_call():
    callcount = [0]

    @memoized('call')
    def foo(d):
        callcount[0] += 1
        return d[0]

    assert foo([0]) == 0
    assert callcount[0] == 1

    assert foo([0]) == 0
    assert callcount[0] == 2


def test_unhashable_arg_error():
    callcount = [0]

    @memoized('error')
    def foo(d):
        callcount[0] += 1
        return d[0]

    with pytest.raises(TypeError):
        assert foo([0]) == 0


def test_unhashable_arg_cPickle():
    callcount = [0]

    @memoized('cPickle')
    def foo(d):
        callcount[0] += 1
        return d[0]

    assert foo([0]) == 0
    assert callcount[0]== 1

    assert foo([0]) == 0
    assert callcount[0]== 1

    assert foo([1]) == 1
    assert callcount[0]== 2
