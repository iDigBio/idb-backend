from __future__ import division, absolute_import, print_function

import pytest
import gevent
import requests

from idigbio_ingestion.mediaing import Status, fetcher
from idb.postgres_backend import apidbpool


def test_continuous(mocker, caplog):
    def get_items(ignores=[], prefix=None):
        items = [
            fetcher.FetchItem('http://prefix.1/1', 'images', 'image/jpg'),
            fetcher.FetchItem('http://prefix.2/2', 'images', 'image/jpg'),
            fetcher.FetchItem('http://prefix.2/2', 'images', 'image/jpg'),
            fetcher.FetchItem('http://prefix.2/2', 'images', 'image/jpg'),
        ]
        return [i for i in items if i.prefix not in ignores]
    mocker.patch.object(fetcher, 'get_items', side_effect=get_items)
    mocker.patch.object(fetcher, "process_list", new=lambda x, forprefix: gevent.sleep(len(x) / 10))
    with gevent.timeout.Timeout(.3, False):
        fetcher.continuous(looptime=0.2)

    assert fetcher.get_items.call_count == 2
    msg = "Starting subprocess for"
    assert len([r for r in caplog.records if r.msg.startswith(msg)]) == 3


def test_once(mocker):
    def get_items(ignores=[], prefix=None):
        items = [
            fetcher.FetchItem('http://prefix.1/1', 'images', 'image/jpg'),
            fetcher.FetchItem('http://prefix.2/2', 'images', 'image/jpg'),
            fetcher.FetchItem('http://prefix.2/2', 'images', 'image/jpg'),
            fetcher.FetchItem('http://prefix.2/2', 'images', 'image/jpg'),
        ]
        return [i for i in items if i.prefix not in ignores]
    mocker.patch.object(fetcher, 'get_items', side_effect=get_items)
    mocker.patch.object(fetcher, "process_list", new=lambda x, forprefix: gevent.sleep(len(x) / 10))


def test_get_items_prefix(mocker):
    "just check it doesn't err"
    mocker.patch.object(apidbpool, 'fetchall', return_value=[])
    fetcher.get_items(prefix="foo")


def test_get_items_ignores(mocker):
    "just check it doesn't err"
    mocker.patch.object(apidbpool, 'fetchall', return_value=[])
    fetcher.get_items(ignores=['foo', 'bar'])


def test_tropicos(mocker):
    TropItem = fetcher.TropicosItem
    mocker.patch.object(TropItem, '_get',
                        side_effect=requests.exceptions.ConnectionError('FauxServerError', '1'))
    mocker.patch.object(fetcher, 'update_db_status', side_effect=lambda x: x)
    items = [
        TropItem('foo', 'images', 'image/jpeg'),
        TropItem('bar', 'images', 'image/jpeg'),
        TropItem('baz', 'images', 'image/jpeg'),
        TropItem('qux', 'images', 'image/jpeg'),
    ]
    with pytest.raises(gevent.timeout.Timeout):
        with gevent.timeout.Timeout(0.5):
            fetcher.process_list(items, forprefix=TropItem.PREFIX)
    assert len([i for i in items if i.status_code is None]) == 3
    assert len([i for i in items if i.status_code == Status.CONNECTION_ERROR]) == 1
