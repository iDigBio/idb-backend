from __future__ import absolute_import

from gevent import monkey
monkey.patch_all()


import time

from idb.postgres_backend import apidbpool

from idigbio_ingestion.lib.log import logger

from idigbio_ingestion import mediaing

mediaing.POOL_SIZE = 2
mediaing.LAST_CHECK_INTERVAL = '10 days'
mediaing.IGNORE_PREFIXES = [
    "http://media.idigbio.org/",
    "http://firuta.huh.harvard.edu/"
]

TROPICOS_URLFILTER = 'http://www.tropicos.org/%'

#All sleep counts are in seconds
BLACKLIST_SLEEP = 49 * 60
RETRY_SLEEP = 18
UNAVAILABLE_SLEEP = 360
RETRIES = 4


def wait_for_new_external_ip(attempt):
    time.sleep(BLACKLIST_SLEEP)


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
            logger.info("Finished   %s successfully", url)
            return 200
        except KeyboardInterrupt:
            raise
        except mediaing.ReqFailure as rf:
            resp = rf.inner.response
            media_status, reason = resp.status_code, resp.reason
            logger.warning("%s on      %s, '%s'", media_status, url, reason)
            if media_status == 404:
                update_status(media_status)
                return media_status
            if media_status == 503:
                time.sleep(UNAVAILABLE_SLEEP)
                continue
            if attempt < RETRIES:
                time.sleep(RETRY_SLEEP)
                continue
            else:
                logger.error("No retries %s", url)
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
        except Exception:
            update_status(1000)
            logger.exception("Unhandled error processing: %s", url)
            return 1000

mediaing.get_media_wrapper = get_media_wrapper


if __name__ == '__main__':
    import logging
    logging.root.setLevel(logging.INFO)
    logging.getLogger('boto').setLevel(logging.WARNING)
    logging.getLogger('requests').setLevel(logging.WARNING)
    mediaing.main(TROPICOS_URLFILTER)
