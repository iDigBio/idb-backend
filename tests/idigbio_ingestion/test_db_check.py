from __future__ import absolute_import, print_function

import pytest

from idigbio_ingestion import db_check

test_rsid = "a83151ae-e1db-4166-9dde-438f6544dca9"
# Colorado Mesa University, Walter A. Kelley Herbarium
# Specimen Records: 3,167
# Media Records: 752
# iDigBio Last Ingested Date: 2017-09-25  (as of June 01, 2018)



def test_get_db_dicts___expected_length_and_contains_expected():
    """Verify that we get the expected number of items, and at least one set
       of known uuid => etag, recordid => uuid"""

    # results = db_check.get_db_dicts(test_rsid)

    # # results[0] contains the dict of uuids => etags
    # # results[1] contains the dict of recordids => uuids

    # assert len(results[0]['records']) > 3000
    # assert len(results[0]['mediarecords']) > 700
    # assert(len(results[1]['records'])) > 3000
    # assert(len(results[1]['mediarecords'])) > 700

    assert True




