from __future__ import division, absolute_import, print_function

from pytz import timezone

import elasticsearch
import elasticsearch.helpers

from idb import config
from idb.helpers.logging import idblogger
from idb.helpers.conversions import fields, custom_mappings

local_tz = timezone('US/Eastern')
logger = idblogger.getChild('indexer')

def get_connection(**kwargs):
    """
    Build connection to ElasticSearch based on json config overriding with specified kwargs

    Returns
    -------
    elasticsearch.Elasticsearch
        An elasticsearch connection object
    """
    kwargs.setdefault('hosts', config.config["elasticsearch"]["servers"])
    kwargs.setdefault('retry_on_timeout', True)  # this isn't valid until >=1.3
    kwargs.setdefault('sniff_on_start', False)
    kwargs.setdefault('sniff_on_connection_fail', False)
    kwargs.setdefault('max_retries', 10)
    kwargs.setdefault('timeout', 30)
    return elasticsearch.Elasticsearch(**kwargs)


def get_indexname(name=config.config["elasticsearch"]["indexname"]):
    """
    Build idigbio indexname, checking for numeric only variant

    Returns
    -------
    string
        idigbio index name in the form of idigbio-#numeric.version#
    """
    if name.startswith("idigbio"):
        return name
    else:
        return "idigbio-" + name


def prepForEs(t, i):
    """
    what does this do?

    Parameters
    ----------
    t : string
        A type such as 'publishers', 'recordsets', 'mediarecords', 'records'
    i : TBD
        The tbd description

    Returns
    -------
    TBD : type
        The description
    """
    value = {}
    for f in fields[t]:
        if f[0] not in i or i[f[0]] is None:
            continue

        if f[2] == "point":
            value[f[0]] = {
                "lon": i[f[0]][0],
                "lat": i[f[0]][1]
            }
        elif f[2] == "date":
            value[f[0]] = i[f[0]].isoformat()
        else:
            # Skip empty values
            try:
                if len(i[f[0]]) > 0:
                    value[f[0]] = i[f[0]]
            except:
                value[f[0]] = i[f[0]]
    return value


class ElasticSearchIndexer(object):
    """
    The Elasticsearch object for managing a connection to the search cluster
    and contains the idigbio indexing methods.
    """

    def __init__(self, indexName, types,
                 commitCount=100000, disableRefresh=True,
                 serverlist=["localhost"]):
        logger.info("Initializing ElasticSearchIndexer(%r, %r, First cluster node: %r)", indexName, types, serverlist[0])
        self.es = get_connection(hosts=serverlist)

        # verify connectivity to cluster
        try:
            self.es.ping()
        except:
            logger.error("Connection failed to cluster. First cluster node: %s", serverlist[0])
            raise SystemExit

        self.indexName = get_indexname(indexName)
        self.types = types
        self.BASECONFIG = {}

        # If in dev environment we are probably using single node elasticsearch
        # on a local machine.  Use single node es config.
        if config.ENV == 'dev':
            self.INDEX_CREATE_SETTINGS = {
                "settings" : {
                    "index" : {
                        "number_of_shards" : 1,
                        "number_of_replicas" : 0
                    }
                }
            }
        else:
            self.INDEX_CREATE_SETTINGS = {
                "settings" : {
                    "index" : {
                        "number_of_shards" : config.ES_INDEX_NUMBER_OF_SHARDS,
                        "number_of_replicas" : config.ES_INDEX_NUMBER_OF_REPLICAS
                    }
                }
            }

        # Create index only if:
        #     1. it does not exist, and 
        #     2. we have the environment variable set to permit index creation.
        self.ALLOW_INDEX_CREATION = True if config.ES_ALLOW_INDEX_CREATION == "yes" else False
        if not self.ALLOW_INDEX_CREATION and not self.es.indices.exists(index=self.indexName):
            logger.info("Index '%s' not found.  If you wish to create it, set ES_ALLOW_INDEX_CREATION=yes environment variable.", self.indexName)
            raise SystemExit
        if self.ALLOW_INDEX_CREATION and not self.es.indices.exists(index=self.indexName):
            logger.info("Creating index: '%s'", self.indexName)
            self.__create_index()
        if self.es.indices.exists(index=self.indexName):
            logger.info("Found index '%s'", self.indexName)

        # We POST the mappings every time an indexer object is created
        # regardless of actual indexing operation we are going to do.
        for t in self.types:
            self.esMapping(t)

        self.commitCount = commitCount
        self.indexedCount = 0
        self.disableRefresh = disableRefresh

        # This is a performance setting so newly indexed documents are not necessarily
        # visible in the index immediately.
        # See close() which sets refresh_interval again.
        if disableRefresh:
            self.es.indices.put_settings(index=self.indexName, body={
                "index": {
                    "refresh_interval": "-1"
                }
            })

    def __create_index(self):
        """
        Create an index with appropriate shard count and replicas for the cluster.
        """
        # create(index, body=None, params=None, headers=None)
        res = self.es.indices.create(index=self.indexName, body=self.INDEX_CREATE_SETTINGS)
        logger.info("Create new Index: %s - %s", self.indexName, res)

    def esMapping(self, t):
        """
        Puts field mappings into Elasticsearch.

        Parameters
        ----------
        t : string
            A type such as 'publishers', 'recordsets', 'mediarecords', 'records'

        """

        m = {
            "date_detection": False,
            "properties": {}
        }
        for f in fields[t]:
            if f[2] == "text":
                m["properties"][f[0]] = {
                    "type": "string", "analyzer": "keyword"}
            elif f[2] == "longtext":
                m["properties"][f[0]] = {"type": "string"}
            elif f[2] == "list":
                m["properties"][f[0]] = {
                    "type": "string", "analyzer": "keyword"}
            elif f[2] == "float":
                m["properties"][f[0]] = {"type": "float"}
            elif f[2] == "boolean":
                m["properties"][f[0]] = {"type": "boolean"}
            elif f[2] == "integer":
                m["properties"][f[0]] = {"type": "integer"}
            elif f[2] == "date":
                m["properties"][f[0]] = {"type": "date"}
            elif f[2] == "point":
                m["properties"][f[0]] = {
                    "type": "geo_point",
                    "geohash": True,
                    "geohash_prefix": True,
                    "lat_lon": True
                }
            elif f[2] == "shape":
                m["properties"][f[0]] = {"type": "geo_shape"}
            elif f[2] == "custom":
                m["properties"][f[0]] = custom_mappings[t][f[0]]
        if t == "mediarecords":
            m["_parent"] = {
                "type": "records"
            }
        res = self.es.indices.put_mapping(index=self.indexName, doc_type=t, body={t: m})
        logger.info("Built mapping for %s: %s", t, res)

    def index(self, t, i):
        """
        Parameters
        ----------
        t : string
            A type such as 'publishers', 'recordsets', 'mediarecords', 'records'
        i : TBD
            something
        """
        if t == "mediarecords" and "records" in i and len(i["records"]) > 0:
            self.es.index(
                index=self.indexName, doc_type=t, id=i["uuid"], parent=i["records"][0], body=i)
        elif t == "mediarecords":
            self.es.index(
                index=self.indexName, doc_type=t, id=i["uuid"], parent=0, body=i)
        else:
            self.es.index(
                index=self.indexName, doc_type=t, id=i["uuid"], body=i)

    def optimize(self):
        """
        Do Nothing.

        Previously ran the es optimize command with the proper number of segments.

        What the heck are the proper number of segments?

        This never returned properly.  In later version of Elasticsearch,
        optimize has been replaced with the "merge" API.

        We can bring this back if it serves a useful purpose.

        TODO: max_num_segments probably needs to be more configurable
        """
        logger.info("Running index optimization on %r", self.indexName)
        logger.info("Skipping index optimization / index merge.")
        # self.es.indices.optimize(index=self.indexName, max_num_segments=5)

    def bulk_formater(self, tups):
        """
        Bulk formats something.
        Needs more info here.
        """
        for t, i in tups:
            meta = {
                "_index": self.indexName,
                "_type": t,
                "_id": i["uuid"],
                "_source": i,
            }
            if i.get("delete", False):
                meta["_op_type"] = "delete"
                del meta["_source"]

            if t == "mediarecords":
                if i.get('delete', False):
                    r = self.query_for_one(i["uuid"], doc_type=t)
                    if r is None:
                        continue   # Delete one that is already not in the index
                    meta["_parent"] = r['_parent']
                elif "records" in i and len(i["records"]) > 0:
                    meta["_parent"] = i["records"][0]
                else:
                    meta["_parent"] = 0

            yield meta

    def bulk_index(self, tups):
        """
        Bulk indexes something.
        Needs more info here.
        """
        return elasticsearch.helpers.streaming_bulk(
            self.es, self.bulk_formater(tups), chunk_size=config.ES_INDEX_CHUNK_SIZE)

    def close(self):
        """
        Finishes index processing.
        """
        # This will allow newly-indexed documents to appear.
        if self.disableRefresh:
            self.es.indices.put_settings(index=self.indexName, body={
                "index": {
                    "refresh_interval": "1s"
                }
            })
        self.optimize()

    def query_for_one(self, uuid, doc_type, source=False):
        """
        What is the point of this?
        """
        r = self.es.search(
            index="idigbio",
            doc_type=doc_type,
            _source=source,
            body={
                "query": {
                    "bool": {
                        "must": [
                            {
                                "query_string": {
                                    "default_field": "_id",
                                    "query": uuid
                                }
                            }
                        ],
                        "must_not": [],
                        "should": []
                    }
                },
                "from": 0,
                "size": 1,
                "sort": [],
                "aggs": {}
            })
        if r['hits']['total'] == 0:
            return None
        if r['hits']['total'] != 1:
            logger.error("Didn't find expected single result for %r", uuid)
        else:
            return r['hits']['hits'][0]
