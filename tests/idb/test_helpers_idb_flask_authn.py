from __future__ import division, absolute_import
from __future__ import print_function, unicode_literals

from idb.helpers.idb_flask_authn import check_auth


def test_check_no_perms(client):
    assert check_auth("2135faef-e12c-4b98-b788-05930d0ca290",
                      "f71cca621ba47b2c5316143de38dc628") is False
