from __future__ import division, absolute_import, print_function
import os
import socket
from celery import Celery
from celery.result import AsyncResult  # noqa
from idb import config, __version__

from idb.helpers.memoize import memoized

env = config.ENV

app = Celery('tasks')
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
    # In python3, redis returns bytes for "everything" by default (both keynames and data).
    # Adding decode_responses=True should force each field name and contents to be a
    # string (instead of bytes) so our string-based comparisons continue to work.
    # e.g. redis_response["status_url"] vs redis_response[b"status_url"]
    # Setting decode_responses fixed the test suite but leaving this comment here
    # as a marker in case production behavior differs.
    return redis.StrictRedis(**get_redis_connection_params(), decode_responses=True)


@app.task()
def version():
    return __version__


@app.task(bind=True)
def healthz(self):
    return {
        "version": __version__,
        "env": env,
        "broker_url": app.conf.broker_url,
        "node": self.request.hostname or os.environ.get("NODENAME") or socket.gethostname()
    }
