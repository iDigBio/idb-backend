from . import *

import json

class RedisRecordSource(object):

    def __init__(self,scan_size=10000,types=["publishers","recordsets","mediarecords","records"]):
        self.scan_size = scan_size     
        self.types = types

    def list_keys(self,filt="*",total=None):
        scan_size = self.scan_size
        if total is not None and total < scan_size:
            scan_size = total        
        e_set = set()

        cursor, elements = redist.scan(0,match=filt,count=scan_size)
        e_set.update(elements)
        while cursor != 0:
            if total is not None and len(e_set) >= total:
                break
            cursor, elements = redist.scan(cursor,match=filt,count=scan_size)
            e_set.update(elements)
        return e_set

    def list_set_values(self,set_name,filt="*",total=None):
        scan_size = self.scan_size
        if total is not None and total < scan_size:
            scan_size = total        
        e_set = set()

        cursor, elements = redist.sscan(set_name,0,match=filt,count=scan_size)
        e_set.update(elements)
        while cursor != 0:
            if total is not None and len(e_set) >= total:
                break
            cursor, elements = redist.sscan(set_name,cursor,match=filt,count=scan_size)
            e_set.update(elements)
        return e_set

    def list_zset_values(self,set_name,filt="*",total=None):
        scan_size = self.scan_size
        if total is not None and total < scan_size:
            scan_size = total        
        e_set = dict()

        cursor, elements = redist.zscan(set_name,0,match=filt,count=scan_size)
        for e in elements:
            e[0] = e[1]
        while cursor != 0:
            if total is not None and len(e_set.keys()) >= total:
                break
            cursor, elements = redist.zscan(set_name,cursor,match=filt,count=scan_size)
            for e in elements:
                e[0] = e[1]
        return e_set        

    def get_key(self,key,force_type=None):
        if force_type is None:
            t = redist.type(key)
        else:
            t = force_type

        if t == "none":
            return None
        elif t == "hash":
            return redist.hgetall(key)
        elif t == "set":
            return self.list_set_values(key)
        elif t == "zset":
            return self.list_zset_values(key)
        elif t == "string":
            return redist.get(key)
        else:
            print t
            return None

    def record_value(self,rid):
        rv = self.get_key("indexer_" + rid,force_type="hash")
        rv["uuid"] = rid
        rv["data"] = json.loads(rv["data"])
        return rv

    def list_type_sets(self):
        for t in self.types:
            yield (t,self.get_key(t))

def main():
    r = RedisRecordSource(scan_size=100000)

    # ks = r.list_keys(total=100)
    # print len(ks)
    # sv = r.list_set_values("mediarecords",total=100)
    # print len(sv)
    # print json.dumps(r.record_value(sv.pop()),indent=2)

    for t, es in r.list_type_sets():
        print t, len(es)

if __name__ == '__main__':
    main()