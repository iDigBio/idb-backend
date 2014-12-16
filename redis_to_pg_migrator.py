import traceback
import requests
from collections import defaultdict

s = requests.Session()

from redis_backend import source
from postgres_backend import sink

def get_etag(t,e):
    r = s.get("http://api.idigbio.org/v1/{0}/{1}".format(t,e))
    r.raise_for_status()
    o = r.json()
    return o["idigbio:etag"]    

def main():
    rrs = source.RedisRecordSource(scan_size=100000)
    prs = sink.PostgresRecordSink()

    results = defaultdict(int)

    for t, es in rrs.list_type_sets():
        print t, len(es)
        print t, prs.run_pre_check(typ=t)
        count = 0
        with prs as tcursor:
            for e in es:   
                try:
                    rv = {
                        "uuid": e,
                        "etag": rrs.record_etag(e),
                        "type": t
                    }

                    if "etag" not in rv or rv["etag"] is None:
                        rv["etag"] = get_etag(t,e)

                    _, needs_update = prs.record_needs_update(rv)

                    if needs_update:
                        rv["data"] = rrs.record_value(e)["data"]
                        results[prs.set_record_value(rv,tcursor=tcursor)] += 1
                    else:
                        results["SKIP"] += 1

                    count += 1

                    if count % 10000 == 0:
                        print t, count, results
                        prs.commit()
                except KeyboardInterrupt, e:
                    raise e
                except:
                    print e
                    traceback.print_exc()
        print t, count, results

if __name__ == '__main__':
    main()