from __future__ import absolute_import

from gevent import monkey
monkey.patch_all()

import time
import requests.exceptions
from idb.postgres_backend import apidbpool
from idigbio_ingestion.lib.log import logger
from idigbio_ingestion import mediaing

mediaing.POOL_SIZE = 1
mediaing.LAST_CHECK_INTERVAL = '10 days'
mediaing.IGNORE_PREFIXES = [
    "http://media.idigbio.org/",
    "http://firuta.huh.harvard.edu/"
]

TROPICOS_URLFILTER = 'http://www.tropicos.org/%'

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
            mediaing.get_media(url, t, fmt)
            logger.info("Success! %s %s", url, 200)
            return 200
        except KeyboardInterrupt:
            raise
        except mediaing.ReqFailure as rf:
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

        except mediaing.ValidationFailure as vf:
            update_status(vf.status)
            if cache_bad:
                mediaing.write_bad(url, vf.content)
            if "IP Address Blocked" in vf.content:
                wait_for_new_external_ip(attempt)
                continue
            logger.error(str(vf))
            return vf.status
        except mediaing.GetMediaError as gme:
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

mediaing.get_media_wrapper = get_media_wrapper


if __name__ == '__main__':
    import logging
    logging.root.setLevel(logging.INFO)
    logging.getLogger('boto').setLevel(logging.WARNING)
    logging.getLogger('requests').setLevel(logging.WARNING)
    mediaing.main(TROPICOS_URLFILTER)
