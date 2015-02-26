import requests
import json

query = {
  "size": 0,
  "aggs": {
    "ccc": {
      "filter": {
        "exists": {
          "field": "countrycode"
        }
      }
    },
    "cc": {
      "terms": {
        "field": "countrycode"
      }
    },
    "kc": {
      "filter": {
        "exists": {
          "field": "kingdom"
        }
      }
    },
    "ka": {
      "filter": {
        "term": {
          "flags": "dwc_kingdom_added"
        }
      }
    },
    "k": {
      "terms": {
        "field": "kingdom"
      }
    },
    "pc": {
      "filter": {
        "exists": {
          "field": "phylum"
        }
      }
    },
    "pa": {
      "filter": {
        "term": {
          "flags": "dwc_phylum_added"
        }
      }
    },
    "p": {
      "terms": {
        "field": "phylum"
      }
    }
  }
}

r = requests.post("http://c18node2.acis.ufl.edu:9200/idigbio-2.0.0/records/_search",data=json.dumps(query))
r.raise_for_status()
o = r.json()


t = float(o["hits"]["total"])
kc = float(o["aggregations"]["kc"]["doc_count"])
ka = float(o["aggregations"]["ka"]["doc_count"])
pc = float(o["aggregations"]["pc"]["doc_count"])
pa = float(o["aggregations"]["pa"]["doc_count"])
ccc = float(o["aggregations"]["ccc"]["doc_count"])

print t, kc/t, ka/t, pc/t, pa/t, ccc/t
