from __future__ import division, absolute_import, print_function

from gevent import monkey
monkey.patch_all()

import time
import requests.exceptions
from idb.postgres_backend import apidbpool
from . import fetcher
from idigbio_ingestion import mediaing

logger = mediaing.logger

fetcher.POOL_SIZE = 1
fetcher.REQ_CONNECT_TIMEOUT = 5 * 60
fetcher.LAST_CHECK_INTERVAL = '10 days'

mediaing.IGNORE_PREFIXES = [
    "http://media.idigbio.org/",
]

TROPICOS_PREFIX = 'http://www.tropicos.org/'

#All sleep counts are in seconds
SLEEP_BLACKLIST = 49 * 60
SLEEP_RETRY = 18
SLEEP_UNAVAILABLE = 360
SLEEP_NOTFOUND = 0.25
RETRIES = 4


def wait_for_new_external_ip(attempt):
    time.sleep(SLEEP_BLACKLIST)


def get_media_wrapper(tup, cache_bad=False):
    "This calls get_media and handles all the failure scenarios"
    url, t, fmt = tup
    logger.debug("Starting on %s", url)
    attempt = 0

    def update_status(status):
        apidbpool.execute(
            "UPDATE media SET last_status=%s, last_check=now() WHERE url=%s",
            (status, url))

    while True:
        attempt += 1
        try:
            fetcher.get_media(url, t, fmt)
            logger.info("Success! %s %s", url, 200)
            return 200
        except KeyboardInterrupt:
            raise
        except fetcher.ReqFailure as rf:
            media_status = rf.status
            reason = rf.response.reason if rf.response is not None else None
            if media_status in (403, 404, 1403):
                logger.info("%s %s '%s'", url, media_status, reason)
                update_status(media_status)
                time.sleep(SLEEP_NOTFOUND)
                return media_status
            elif media_status in (503,):
                logger.warning("Remote Service Unavailable. %s %s '%s'", url, media_status, reason)
                time.sleep(SLEEP_UNAVAILABLE)
                continue
            elif attempt < RETRIES:
                logger.warning("Will Retry. %s %s '%s'", url, media_status, reason)
                time.sleep(SLEEP_RETRY)
                continue
            else:
                logger.error("No More Retries. %s %s '%s'", url, media_status, reason)
                time.sleep(1)
                update_status(media_status)
                return media_status

        except fetcher.ValidationFailure as vf:
            update_status(vf.status)
            if cache_bad:
                fetcher.write_bad(url, vf.content)
            if "IP Address Blocked" in vf.content:
                wait_for_new_external_ip(attempt)
                continue
            logger.error(str(vf))
            return vf.status
        except fetcher.GetMediaError as gme:
            update_status(gme.status)
            logger.error(str(gme))
            return gme.status
        except requests.exceptions.ConnectionError as connectione:
            logger.warning(
                "Connection Error. %s %s %s",
                url, connectione.errno, connectione.message)
            time.sleep(SLEEP_UNAVAILABLE)
            continue
        except Exception:
            update_status(1000)
            logger.exception("*Unhandled error processing* %s", url)
            return 1000

fetcher.get_media_wrapper = get_media_wrapper
