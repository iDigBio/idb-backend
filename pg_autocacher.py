import traceback
import requests
import time
from collections import defaultdict

from redis_backend import redist
from postgres_backend import sink

s = requests.Session()

def get_data(t,e):
    r = s.get("http://api.idigbio.org/v1/{0}/{1}".format(t,e))
    r.raise_for_status()
    o = r.json()
    return o    

def main():
    prs = sink.PostgresRecordSink()

    results = defaultdict(int)

    p = redist.pubsub()

    p.psubscribe("cacher_*")

    while True:
        message = p.get_message()
        if message is not None:
            t = message["channel"][len("cacher_"):]
            e = redist.spop("cacher_" + t + "_queue")
            if e is not None:
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
                            redist.sadd("pg_incremental_indexer_" + t + "_queue", e)
                            redist.publish("pg_incremental_indexer_" + t, e)
                        else:
                            results["SKIP"] += 1
                    except KeyboardInterrupt:
                        break
                    except:
                        redist.sadd("cacher_" + t + "_queue",e)
                        print e
                        traceback.print_exc()
                print results
            else:
                time.sleep(1)
        else:
            time.sleep(1)


if __name__ == '__main__':
    main()