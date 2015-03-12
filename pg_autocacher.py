import traceback
import requests
import time
from collections import defaultdict

import os

from redis_backend import redist
from redis_backend.queue import RedisQueue
from postgres_backend import sink
from config import config

s = requests.Session()

def get_data(t,e):
    r = s.get("http://api.idigbio.org/v1/{0}/{1}".format(t,e))
    r.raise_for_status()
    o = r.json()
    return o

def cache_item(prs,q,iq,t,e):
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
                return prs.set_record_value(rv,tcursor=tcursor)
                iq.add(t,e)
            else:
                return "SKIP"
        except KeyboardInterrupt:
            return
        except requests.exceptions.HTTPError, e:
            if e.response.status_code != 404
                q.add(t,e)
                print t, e
                traceback.print_exc()
        except:
            q.add(t,e)
            print t, e
            traceback.print_exc()            

def main():
    prs = sink.PostgresRecordSink()
    q = RedisQueue(queue_prefix="cacher_")
    iq = RedisQueue(queue_prefix="pg_incremental_indexer_")

    results = defaultdict(int)

    count = 0

    for t in config["elasticsearch"]["types"]:
        count += 1
        print "Drain", t
        for t, e in q.drain(t):
            results[cache_item(prs,q,iq,t,e)] += 1
            if count % 1000 == 0:
                print os.getpid(), results, dict(q.hwm)            

    print "Listen"
    for t,e in q.listen():
        count += 1
        results[cache_item(prs,q,iq,t,e)] += 1
        if count % 1000 == 0:
            print os.getpid(), results, dict(q.hwm)


if __name__ == '__main__':
    main()