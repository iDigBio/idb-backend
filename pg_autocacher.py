import traceback
import requests
import time
from collections import defaultdict

import os

from redis_backend import redist
from redis_backend.queue import RedisQueue
from postgres_backend import sink

s = requests.Session()

def get_data(t,e):
    r = s.get("http://api.idigbio.org/v1/{0}/{1}".format(t,e))
    r.raise_for_status()
    o = r.json()
    return o    

def main():
    prs = sink.PostgresRecordSink()
    q = RedisQueue(queue_prefix="cacher_")
    iq = RedisQueue(queue_prefix="pg_incremental_indexer_")

    results = defaultdict(int)

    count = 0
    for t,e in q.listen(sleep_time=1):
        count += 1
        with prs as tcursor:
            try:
                rv = {
                    "uuid": e,
                    "type": t
                }

                rv["data"] = get_data(t,e)
                rv["etag"] = rv["data"]["idigbio:etag"]

                _, needs_update = prs.record_needs_update(rv)

                if needs_update:
                    results[prs.set_record_value(rv,tcursor=tcursor)] += 1
                    iq.add(t,e)
                else:
                    results["SKIP"] += 1
            except KeyboardInterrupt:
                break
            except:
                q.add(t,e)
                print t, e
                traceback.print_exc()
        if count % 1000 == 0:
            print os.getpid(), results


if __name__ == '__main__':
    main()