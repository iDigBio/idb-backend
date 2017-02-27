from __future__ import division, absolute_import, print_function
import os
from celery import Celery
from celery.result import AsyncResult
from idb import config

from idb.helpers.memoize import memoized

app = Celery('tasks')
env = config.ENV
app.config_from_object('idigbio_workers.config.' + env)

# this must be imported so it has a chance to register worker tasks.
from idigbio_workers.tasks.download import downloader, blocker, send_download_email  # noqa


@memoized()
def get_redis_connection_params():
    from kombu.utils.url import _parse_url
    url = app.conf['broker_url']
    scheme, host, port, user, password, path, query = _parse_url(url)
    return {'host': host, 'port': port, 'db': path}


def get_redis_conn():
    import redis
    return redis.StrictRedis(**get_redis_connection_params())


@app.task()
def version():
    import idb
    return idb.__version__
