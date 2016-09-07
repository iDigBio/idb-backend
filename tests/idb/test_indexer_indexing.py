from __future__ import division, absolute_import, print_function
import pytest

from idb.indexing import indexer


def test_get_indexname():
    assert "idigbio" == indexer.get_indexname('idigbio')
    assert "idigbio-2.10.0" == indexer.get_indexname('idigbio-2.10.0')
    assert "idigbio-2.10.0" == indexer.get_indexname('2.10.0')
