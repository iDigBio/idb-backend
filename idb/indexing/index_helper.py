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


# A problematic field name is "http://rs.iobis.org/obis/terms/measurementTypeID" inside
# the "obis:ExtendedMeasurementOrFact".
UNINDEXABLE_OBJECTS = ["obis:ExtendedMeasurementOrFact", "chrono:ChronometricAge"]

def index_record(ei, rc, typ, r, do_index=True):
    """
    Index a single database record.

    Parameters
    ----------
    ei : ElasticSearchIndexer
    rc : RecordCorrector
    typ : string
        A type such as 'publishers', 'recordsets', 'mediarecords', 'records'
    r : the record data object
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
        # d : corrected_dict, ck : corrected_keys
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

        # Fixup problematic field names due to limitations of Elasticsearch.
        # For example, dots in fieldnames.
        for k in r["data"]:
            if "." in k:
                if config.IDB_EXTRA_SERIOUS_DEBUG == 'yes':
                    logger.debug("type: '{0}'".format(k))
                if k in types:
                    r["data"][types[k]["shortname"]] = r["data"][k]
                    del r["data"][k]
                else:
                    # If it has a dot, we're assuming its URL-like
                    urldata = urlparse(k)
                    try:
                        # Prefix = primary domain component i.e gbif of gbif.org
                        prefix = urldata.hostname.split()[-2]
                    except IndexError:
                        # Not Actually URL-like, bad assumption!
                        logger.error("Could not parse type from: '{0}', '{1}'".format(urldata, k))
                        # this still needs to be fatal at this point since we don't know what to do
                        raise
                    # Suffix = last component of path
                    suffix = urldata.path.split("/")[-1]
                    r["data"][prefix + ":" + suffix] = r["data"][k]
                    d["flag_data_" + prefix + "_" + suffix + "_munge"] = True
            if k in UNINDEXABLE_OBJECTS:
                # inventing a new flag for each field we are truncating
                new_flag = "_".join(["flag", "idigbio", k.replace(":","_").lower(), "truncated"])
                d[new_flag] = True
                # truncate the troublesome object to prevent Elasticsearch mapper_parsing_exception
                d[k] = {}
                r["data"][k] = {}

        i["data"] = r["data"]
        i["indexData"] = d

        if config.IDB_EXTRA_SERIOUS_DEBUG == 'yes':
            logger.debug("Index record: %s with approx. %s bytes of data.", i["uuid"], len(repr(i)))
            logger.debug("Data: %s", repr(i))

        if do_index:
            ei.index(typ, i)
        else:
            return (typ, i)
