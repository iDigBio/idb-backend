from conversions import grabAll
from elasticsearch_backend.indexer import prepForEs
from postgres_backend.db import tombstone_etag


def index_record(ei,rc,typ,r,do_index=True):

        if r["etag"] == tombstone_etag:
            i = {
                "uuid": r["uuid"],
                "delete": True,
                "parent": r["parent"]
            }
            return (typ,i)
        else:
            d,ck = rc.correct_record(r["data"])

            d.update({
                "idigbio:uuid": r["uuid"],
                "idigbio:etag": r["etag"],
                "idigbio:siblings": r["siblings"] if "siblings" in r and r["siblings"] is not None else {},
                "idigbio:recordIds": r["recordids"],
                "idigbio:parent": r["parent"],
                "idigbio:dateModified": r["modified"].isoformat()
            })

            g = grabAll(typ,d)
            i =  prepForEs(typ,g)
            i["data"] = r["data"]

            if do_index:
                ei.index(typ,i)
            else:
                return (typ,i)