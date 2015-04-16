import unittest

import os
import sys

sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.realpath(__file__)))))

from helpers.idb_flask_authn import *
import copy

class TestCheckAuth(unittest.TestCase):
    def test_check_no_perms(self):
        assert check_auth("2135faef-e12c-4b98-b788-05930d0ca290","f71cca621ba47b2c5316143de38dc628") is False
        


if __name__ == '__main__':
    unittest.main()
