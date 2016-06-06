"""Tests for the idigbio_ingestion mediaing process

As that process is a lot of side effects there isn't much to easily do
here. At the minimum this imports that module: syntax check.

"""

import pytest

from idigbio_ingestion import mediaing


def test_check_ignore_media():
    ip = mediaing.IGNORE_PREFIXES[0]
    assert mediaing.check_ignore_media(ip + "asdf")
    assert not mediaing.check_ignore_media("asdfasdf")
