from conversions import grabAll
from elasticsearch_backend.indexer import prepForEs

def index_record(ei,rc,typ,r,do_index=True):
        d,ck = rc.correct_record(r["data"]["idigbio:data"])

        d.update(r["data"])
        del d["idigbio:data"]

        i =  prepForEs(typ,grabAll(typ,d))
        i["data"] = r["data"]

        if do_index:
            ei.index(typ,i)
        else:
            return (typ,i)