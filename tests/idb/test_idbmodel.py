from __future__ import division, absolute_import, print_function
import pytest

@pytest.mark.xfail
def test_insert_and_delete(logger, testidbmodel):
    test_uuid = "00000000-0000-0000-0000-000000000000"
    testidbmodel.set_record(test_uuid, 'record', None, {}, [], [])
    testidbmodel.delete_item(test_uuid)
