# Will immediately perform database connection checks on import.
# This is to fail fast with immediate error reporting,
# rather than on first database request.
#
# An attempt was previously made to de-escalate
# 'Client sent AUTH, but no password is set.' errors to warnings instead by
# resetting ({Celery}app).conf.broker_url in this module.
# However, configuration changes are not propagated to tasks,
# which will still use a broker_url with a password.

from __future__ import division, absolute_import, print_function
import os
import socket
import sys
from celery import bootsteps, Celery, signals
from celery.result import AsyncResult  # noqa
from kombu.utils.url import _parse_url, as_url, sanitize_url
from idb import config, __version__
from idb.helpers.memoize import memoized

# True on command lines matching '*celery*worker*'.
IN_CELERY_WORKER_PROCESS = sys.argv and sys.argv[0].endswith('celery') and 'worker' in sys.argv
if IN_CELERY_WORKER_PROCESS:
    from celery.utils.log import get_logger

    # Ineffectual until Celery signals.after_setup_logger
    logger = get_logger('idigbio_workers_init')

    @signals.after_setup_logger.connect
    def handle_celery_after_setup_logger(**kwargs):
        global logger
        logger = kwargs['logger']
        _correct_password_urlpart(app)
else:
    from idb.helpers.logging import idblogger
    logger = idblogger.getChild('idigbio_workers_init')

env = config.ENV

app = Celery('tasks')
app.config_from_object('idigbio_workers.config.' + env)

class PreConsumerConnectionStep(bootsteps.StartStopStep):
    """When this bootstep is run, immediately triggers broker connectivity check and fail fast if there is an issue."""
    requires = {'celery.worker.components:Pool'}

    def start(self, worker):
        _ = get_redis_connection_params()
app.steps['worker'].add(PreConsumerConnectionStep)

def _correct_password_urlpart(app):
    #type: (Celery) -> None
    """Corrects configuration values for 'broker_url' and 'result_backend'
    to use passwords from idigbio.json, if applicable.
    """
    from idb import config
    keep_result_backend = app.conf['broker_url'] != app.conf['result_backend'] #type: bool
    if keep_result_backend:
        logger.warning("Celery config 'broker_url' does not match 'result_backend'. "
            "Password corrections will not be applied to 'result_backend' and connection errors may occur.")
    urlparts = _parse_url(app.conf['broker_url'])
    if urlparts.password:
        logger.warning("Password ignored in Celery config 'broker_url'. "
            "Please use environment variable 'IDB_REDIS_AUTH' instead.")
        #reason: Contributors may be more likely to accidentally check in & leak the authentication key
        # in the config/*.py versus the dedicated idigbio.json secrets file.
    urlparts = urlparts._replace(password=config.IDB_REDIS_AUTH)
    app.conf['broker_url'] = as_url(*urlparts)
    if not keep_result_backend:
        app.conf['result_backend'] = app.conf['broker_url']
if not IN_CELERY_WORKER_PROCESS:
    # If in celery worker, delay until logging is set up (called in handle_celery_after_setup_logger())
    # Has to be done as soon as possible, otherwise tasks may be passed an incorrect broker password.
    _correct_password_urlpart(app)


@memoized()
def get_redis_connection_params():
    """
    Also performs one-time parameter validation (only for redis:// broker URLs):
    a PING command is sent. If it fails, an exception may be raised.
    """
    # see also: logging in _correct_password_urlpart()
    from kombu.utils.url import _parse_url
    # below url may contain an inline plaintext password
    url_sensitive = app.conf['broker_url']
    scheme, host, port, user, password, path, query = _parse_url(url_sensitive)
    redis_params = {'host': host, 'port': port, 'db': path, 'password': password}

    # Since this is memoized, this will run once.
    # Might as well check if these params actually work now,
    # rather than getting a less-descriptive exception from Celery later.
    if scheme not in ('redis','rediss'):
        # ...except we're not working with redis://,
        # so the following test shall be skipped
        logger.warning("Connectivity check for Celery config broker_url='%s' skipped: non-redis:// URL", sanitize_url(url_sensitive))
        return redis_params

    import redis

    try:
        conn = redis.StrictRedis(**redis_params)
        if conn.ping():
            logger.info('Redis connection ping OK -- %s', sanitize_url(url_sensitive))
        else:
            logger.error('Redis connection ping failed')
    except Exception as exc:
        logger.error('Redis connection ping error: %r', exc)
        raise
    return redis_params
if not IN_CELERY_WORKER_PROCESS:
    print(get_redis_connection_params())

# This must be imported so it has a chance to register worker tasks.
# If later doing app broker_url configuration changes at runtime,
# these imports should probably be done after those changes have been executed
# (otherwise incorrect redis params may be used).
from idigbio_workers.tasks.download import downloader, blocker, send_download_email  # noqa

def get_redis_conn():
    import redis
    return redis.StrictRedis(**get_redis_connection_params())


@app.task()
def version():
    return __version__


@app.task(bind=True)
def healthz(self):
    return {
        "version": __version__,
        "env": env,
        "broker_url": sanitize_url(app.conf.broker_url),
        "node": self.request.hostname or os.environ.get("NODENAME") or socket.gethostname()
    }
