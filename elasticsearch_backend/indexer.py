from pytz import timezone

import elasticsearch.helpers
from elasticsearch import Elasticsearch

from helpers.conversions import fields

local_tz = timezone('US/Eastern')

def prepForEs(t,i):
    value = {}
    for f in fields[t]:            
        if f[0] not in i or i[f[0]] == None:
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
            if len(i[f[0]]) > 0:
                value[f[0]] = i[f[0]]
    return value

class ElasticSearchIndexer(object):
    def __init__(self,indexName,types,commitCount=100000,disableRefresh=True,serverlist=["localhost"]):
        self.es = elasticsearch.Elasticsearch(serverlist, sniff_on_start=True, sniff_on_connection_fail=True,retry_on_timeout=True,max_retries=3)
        self.types = types
        self.indexName = "idigbio-" + indexName

        for t in self.types:
            self.esMapping(t)

        self.commitCount = commitCount
        self.indexedCount = 0    
        self.disableRefresh = disableRefresh    

        if disableRefresh:
            self.es.indices.put_settings(index=self.indexName,body={
                "index" : {
                    "refresh_interval" : "-1"
                }
            })

    def esMapping(self,t):
        m = {
            "date_detection": False,
            "properties": {}
        }
        for f in fields[t]:
            if f[2] == "text":
                m["properties"][f[0]] = { "type" : "string", "analyzer": "keyword" }
            elif f[2] == "longtext":
                m["properties"][f[0]] = { "type" : "string" }             
            elif f[2] == "list":
                m["properties"][f[0]] = { "type" : "string", "analyzer": "keyword" }           
            elif f[2] == "float":
                m["properties"][f[0]] = { "type" : "float" }
            elif f[2] == "boolean":
                m["properties"][f[0]] = { "type": "boolean" }
            elif f[2] == "integer":
                m["properties"][f[0]] = { "type" : "integer" }
            elif f[2] == "date":
                m["properties"][f[0]] = { "type": "date" }
            elif f[2] == "point":
                m["properties"][f[0]] = { "type": "geo_point", "geohash": True, "geohash_prefix": True, "lat_lon": True}
        if t == "mediarecords":
            m["_parent"] = {
                "type" : "records"
            }        
        print self.es.indices.put_mapping(index=self.indexName,doc_type=t,body={ t: m })

    def index(self,t,i):
        if t == "mediarecords" and "records" in i and len(i["records"]) > 0:
            self.es.index(index=self.indexName,doc_type=t,id=i["uuid"],parent=i["records"][0],body=i)
        elif t == "mediarecords":
            self.es.index(index=self.indexName,doc_type=t,id=i["uuid"],parent=0,body=i)
        else:
            self.es.index(index=self.indexName,doc_type=t,id=i["uuid"],body=i)

    def bulk_formater(self,tups):
        for t,i in tups:
            meta = {
                "_index": self.indexName,
                "_type": t,
                "_id": i["uuid"],
                "_source": i,
            }
            if t == "mediarecords":
                if "records" in i and len(i["records"]) > 0:            
                    meta["_parent"] = i["records"][0]
                else:
                    meta["_parent"] = 0
            yield meta

    def bulk_index(self,tups):
        return elasticsearch.helpers.streaming_bulk(self.es,self.bulk_formater(tups))       

    def close(self):
        if self.disableRefresh:
            self.es.indices.put_settings(index=self.indexName,body={
                "index" : {
                    "refresh_interval" : "1s"
                }
            })            
        self.es.indices.optimize(index=self.indexName,max_num_segments=5)