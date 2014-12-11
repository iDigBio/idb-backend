import traceback
import requests

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
    prs = sink.PostgresRecordSink(skip_existing=True)

    for t, es in rrs.list_type_sets():
        print t, len(es)
        count = 0
        with prs as tcursor:
            for e in es:   
                try:    
                    rv = rrs.record_value(e)
                    rv["type"] = t

                    if "etag" not in rv:
                        rv["etag"] = get_etag(t,e)

                    prs.set_record_value(rv,tcursor)
                    count += 1

                    if count % 10000 == 0:
                        print t, count
                        prs.commit()
                except:
                    print e
                    traceback.print_exc()
        print t, count


if __name__ == '__main__':
    main()