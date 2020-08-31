from __future__ import absolute_import
from idb.helpers.conversions import grabAll
from idb.postgres_backend.db import tombstone_etag
from idb import config

from .indexer import prepForEs
from idb.helpers.fieldnames import types

from idb.helpers.logging import idblogger
logger = idblogger.getChild('index_helper')

# PYTHON3_WARNING
from urlparse import urlparse

def index_record(ei, rc, typ, r, do_index=True):
    """
    Index a single database record.

    Parameters
    ----------
    ei : ElasticSearchIndexer
    rc : RecordCorrector
    t : string
        A type such as 'publishers', 'recordsets', 'mediarecords', 'records'
    do_index : boolean
        Actually update the index or not.
    """
    if r["etag"] == tombstone_etag:
        i = {
            "uuid": r["uuid"],
            "delete": True,
        }
        if typ == "mediarecords":
            sibs = r.get('siblings')
            if sibs:
                i["records"] = sibs.get('record', [])
        return (typ, i)
    else:
        d, ck = rc.correct_record(r["data"])

        d.update({
            "idigbio:uuid": r["uuid"],
            "idigbio:etag": r["etag"],
            "idigbio:siblings": r["siblings"] if "siblings" in r and r["siblings"] is not None else {},
            "idigbio:recordIds": r["recordids"],
            "idigbio:parent": r["parent"],
            "idigbio:dateModified": r["modified"].isoformat()
        })

        g = grabAll(typ, d)
        i = prepForEs(typ, g)

        # Remove fieldnames with dots in them
        # to fix an issue converting to ES 2.3+
        for k in r["data"]:
            if "." in k:
                if k in types:
                    r["data"][types[k]["shortname"]] = r["data"][k]
                    del r["data"][k]
                else:
                    # If it has a dot, we're assuming its URL-like
                    urldata = urlparse(k)
                    # Prefix = primary domain component i.e gbif of gbif.org
                    prefix = urldata.hostname.split()[-2]
                    # Suffix = last component of path
                    suffix = urldata.path.split("/")[-1]
                    r["data"][prefix + ":" + suffix] = r["data"][k]
                    d["flag_data_" + prefix + "_" + suffix + "_munge"] = True

        i["data"] = r["data"]
        i["indexData"] = d

        if config.IDB_EXTRA_SERIOUS_DEBUG == 'yes': # dummy comment because github
            logger.debug("Index record: %s with approx. %s bytes of data.", i["uuid"], len(repr(i)))
            logger.debug("Data: %s", repr(i))

        if do_index:
            ei.index(typ, i)
        else:
            return (typ, i)
