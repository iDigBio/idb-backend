from __future__ import print_function
import os
from collections import Counter

import logging
logging.basicConfig()
from idb.helpers.logging import idblogger, configure
logger = idblogger.getChild("taxon_index")
configure(logger=logger)

from idigbio_ingestion.lib.dwca import Dwca

import elasticsearch.helpers
from elasticsearch import Elasticsearch

index = "taxonnames-20170619"

es = Elasticsearch([
    "c18node2.acis.ufl.edu",
    "c18node6.acis.ufl.edu",
    "c18node10.acis.ufl.edu",
    "c18node12.acis.ufl.edu",
    "c18node14.acis.ufl.edu"
], sniff_on_start=False, sniff_on_connection_fail=False, retry_on_timeout=True, max_retries=10, timeout=10)

def bulk_formater(tups):
    for t, i in tups:
        meta = {
            "_index": index,
            "_type": t,
            "_id": "gbif_" + i["dwc:taxonID"],
            "_source": i,
        }
        yield meta

def bulk_index(tups):
    return elasticsearch.helpers.streaming_bulk(es, bulk_formater(tups), chunk_size=10000)


def get_data(path_to_checklist):
    rts = Counter()
    d = Dwca(path_to_checklist, logname="taxon_index")
    last_row = {}
    for r in d.core:
        rts["rows"] += 1
        #print last_row
        if "id" in r:
            rid = int(r["id"])
            for e in d.extensions:
                rts[e.rowtype] += 1

                if e.rowtype in last_row:
                    er = last_row[e.rowtype]
                    cid = int(er["coreid"])
                    if cid > rid:
                        continue  # Skip this extension for this row
                    elif cid == rid:
                        #del er["coreid"]
                        rts[e.rowtype + "_add"] += 1
                        if e.rowtype in r:
                            r[e.rowtype].append(er)
                        else:
                            r[e.rowtype] = [er]
                    else:
                        pass

                for er in e:
                    cid = er.get("coreid")
                    if cid is not None:
                        cid = int(cid)
                        if cid > rid:
                            last_row[e.rowtype] = er
                            break
                        elif cid == rid:
                            #del er["coreid"]
                            rts[e.rowtype + "_add"] += 1
                            if e.rowtype in r:
                                r[e.rowtype].append(er)
                            else:
                                r[e.rowtype] = [er]
                        else:
                            pass

            r["dwc:taxonID"] = r["id"]
            del r["id"]
            yield ("taxonnames",r)
        if rts["rows"] % 10000 == 0:
            print (rts.most_common())

    print (rts.most_common())

def main():
    # for r in get_data(os.path.expanduser("~/Downloads/backbone-current-sorted.zip")):
    #     pass
    for _ in bulk_index(get_data(os.path.expanduser("~/Downloads/backbone-current-sorted.zip"))):
        pass


if __name__ == '__main__':
    main()
