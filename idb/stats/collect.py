from __future__ import division, absolute_import, print_function
import json

from collections import defaultdict
from datetime import datetime, timedelta

from idb.postgres_backend import apidbpool, DictCursor
from idb.postgres_backend.stats_db import statsdbpool
from idb.indexing.indexer import get_connection
from idb.helpers.logging import idblogger
from idb.stats import indexName, typeName

logger = idblogger.getChild('stats')

record_types = ["records", "mediarecords"]
stat_types = ["download", "mapping", "search", "seen", "view"]

STAT_TYPE_PROPERTIES = {
    "properties": {
        "count": {"type": "double"},
        "total": {"type": "double"},
        "items": {
            "properties": {
                "count": {"type": "double"},
                "term": {"type": "string", "analyzer": "keyword"}
            }
        },
        "queries": {
            "properties": {
                "count": {"type": "double"},
                "term": {"type": "string", "analyzer": "keyword"}
            }
        },
        "geocodes": {
            "properties": {
                "count": {"type": "double"},
                "geo": {
                    "properties": {
                        "country": {"type": "string", "analyzer": "keyword"},
                        "region": {"type": "string", "analyzer": "keyword"},
                        "city": {"type": "string", "analyzer": "keyword"},
                    }
                }
            }
        }
    }
}

def get_search_stats_mapping():
    search_stats_mapping = {
        "properties": {
            "recordset_id": {"type": "string", "analyzer": "keyword"},
            "harvest_date": {"type": "date"}
        }
    }

    for record_type in record_types:
        search_stats_mapping["properties"][record_type] = {
            "properties": {
                stat_type: STAT_TYPE_PROPERTIES for stat_type in stat_types
            }
        }
    return search_stats_mapping


def put_search_stats_mapping(es=None):
    logger.info("Putting search stats mapping into index: %r, doc_type: %r",
                indexName, typeName)
    es = es or get_connection()
    es.indices.put_mapping(
        index=indexName,
        doc_type=typeName,
        body={typeName: get_search_stats_mapping()})


def new_stats_dict():
    stats_dict = {}
    for record_type in record_types:
        stats_dict[record_type] = {}
        for stat_type in stat_types:
            stats_dict[record_type][stat_type] = {"count": 0, "total": 0}
            stats_dict[record_type][stat_type]["items"] = defaultdict(int)
            stats_dict[record_type][stat_type]["queries"] = defaultdict(int)
            stats_dict[record_type][stat_type]["geocodes"] = defaultdict(int)
    return stats_dict


def get_stats_dates():
    sql = """SELECT date_trunc('day', date)
             FROM stats
             GROUP BY date_trunc('day', date)
             ORDER BY date_trunc('day', date)"""

    return [r[0] for r in statsdbpool.fetchall(sql)]


def collect_stats(collect_datetime, es=None):
    es = es or get_connection()
    date_min = (collect_datetime - timedelta(1)).date()
    date_max = collect_datetime.date()

    # Trap duplicate runs and abort.
    c = es.count(index="stats", doc_type="search", body={"query": {
        "term": {
            "harvest_date": date_max.isoformat()
        }
    }})
    if c["count"] != 0:
        logger.warn("Duplicate run detected, aborting")
        return

    logger.info("Collecting stats for %s to %s", date_min, date_max)
    recordset_stats = defaultdict(new_stats_dict)

    # print date_min, date_max
    sql = """SELECT * FROM stats
             LEFT JOIN queries on stats.query_id=queries.id
             WHERE date > %s AND date < %s
    """
    logger.debug("min is %s, max is %s, query is: %s" % (date_min, date_max, sql))

    filename = "telem-output-structures-%s.log" % (datetime.now().strftime("%Y-%m-%d-%H-%M-%S"))
    tracefilename = "trace-log-%s.log" % (datetime.now().strftime("%Y-%m-%d-%H-%M-%S"))

    # collects the raw objects we're sending to ES
    output_fh = open(filename, 'w')
    
    trace_fh = open(tracefilename, 'w')

    trace_fh.write("doingquery and looping through results...")

    for r in statsdbpool.fetchiter(sql, (date_min, date_max), cursor_factory=DictCursor):
        record_type = r["record_type"]
        stats_type = r["type"]
        query_hash = r["query_hash"]
        geocode = json.dumps(r["ip_geocode"], sort_keys=True)

        trace_fh.write("record_type: [%s] stats_type: [%s] query_hash: [%s] geocode: %s \n\n" % (record_type, stats_type, query_hash, geocode))

        if record_type in record_types:
            if stats_type == "view":
                for record_key in r["payload"]:
                    recordset_key = r["payload"][record_key]
                    recordset_stats[recordset_key][record_type][stats_type]["count"] += 1
                    recordset_stats[recordset_key][record_type][stats_type]["total"] += 1
                    recordset_stats[recordset_key][record_type][stats_type]["items"][record_key] += 1
                    recordset_stats[recordset_key][record_type][stats_type]["geocodes"][geocode] += 1
            elif stats_type == "seen":
                for record_key in r["payload"]:
                    recordset_key = r["payload"][record_key]
                    recordset_stats[recordset_key][record_type][stats_type]["count"] += 1
                    recordset_stats[recordset_key][record_type][stats_type]["total"] += 1
                    recordset_stats[recordset_key][record_type][stats_type]["items"][record_key] += 1
                    recordset_stats[recordset_key][record_type][stats_type]["queries"][query_hash] += 1
                    recordset_stats[recordset_key][record_type][stats_type]["geocodes"][geocode] += 1
            else:
                for recordset_key in r["payload"]:
                    record_count = r["payload"][recordset_key]
                    recordset_stats[recordset_key][record_type][stats_type]["count"] += 1
                    recordset_stats[recordset_key][record_type][stats_type]["total"] += record_count
                    recordset_stats[recordset_key][record_type][stats_type]["queries"][query_hash] += record_count
                    recordset_stats[recordset_key][record_type][stats_type]["geocodes"][geocode] += record_count

    logger.info("finished with first loop through resultset for sums. Now beginning second loop.")
    trace_fh.write("finished with first loop through resultset for sums. Now beginning second loop.")

    for recordset_key in recordset_stats:
        recordset_data = {
            "recordset_id": recordset_key,
            "harvest_date": collect_datetime.date().isoformat()
        }
        for record_type in record_types:
            recordset_data[record_type] = {}
            for stat_type in stat_types:
                log_line = "\n\n###############################\nbeginning second loop for record_type: [%s] stat_type: [%s] \n" % (record_type, stat_type)
                trace_fh.write(log_line)

                recordset_data[record_type][stat_type] = {
                    "count": recordset_stats[recordset_key][record_type][stat_type]["count"],
                    "total": recordset_stats[recordset_key][record_type][stat_type]["total"],
                    "items": [],
                    "queries": [],
                    "geocodes": []
                }

                log_line = "recordset_data[record_type][stat_type] recordset_data[%s][%s]  is:\n%s" % (record_type, stat_type, recordset_data[record_type][stat_type])
                trace_fh.write(log_line)

                for k in recordset_stats[recordset_key][record_type][stat_type]["items"]:
                    trace_fh.write("now looping through recordset_stats[recordset_key: %s][%s][%s][items] . \n" % (recordset_key, record_type, stat_type))
                    trace_fh.write("k is: %s\n" %(k))
                    trace_fh.write("count (recordset_stats[recordset_key][record_type][stat_type][items][k]) is: %s\n\n" % (recordset_stats[recordset_key][record_type][stat_type]["items"][k]))
                    
                    recordset_data[record_type][stat_type]["items"].append({
                        "term": k,
                        "count": recordset_stats[recordset_key][record_type][stat_type]["items"][k]
                    })
                for k in recordset_stats[recordset_key][record_type][stat_type]["queries"]:
                    trace_fh.write("now looping through recordset_stats[recordset_key: %s][%s][%s][items] . \n" % (recordset_key, record_type, stat_type))
                    trace_fh.write("k is: %s\n" %(k))
                    trace_fh.write("count (recordset_stats[recordset_key][record_type][stat_type][queries][k]) is: %s\n\n" % (recordset_stats[recordset_key][record_type][stat_type]["queries"][k]))
                    

                    recordset_data[record_type][stat_type]["queries"].append({
                        "term": k,
                        "count": recordset_stats[recordset_key][record_type][stat_type]["queries"][k]
                    })
                for k in recordset_stats[recordset_key][record_type][stat_type]["geocodes"]:
                    trace_fh.write("now looping through recordset_stats[recordset_key: %s][%s][%s][items] . \n" % (recordset_key, record_type, stat_type))
                    trace_fh.write("json.loads(k) is: %s\n" %( json.loads(k)))
                    trace_fh.write("count (recordset_stats[recordset_key][record_type][stat_type][geocodes][k]) is: %s\n\n" % (recordset_stats[recordset_key][record_type][stat_type]["geocodes"][k]))
                    
                    recordset_data[record_type][stat_type]["geocodes"].append({
                        "geo": json.loads(k),
                        "count": recordset_stats[recordset_key][record_type][stat_type]["geocodes"][k]
                    })
        trace_fh.write("writing recordset_data to ES: \n")
        output_fh.write(json.dumps(recordset_data))
        trace_fh.write(json.dumps(recordset_data))
        
        es.index(index=indexName, doc_type=typeName, body=recordset_data)
    
    logger.info("end of script")


def api_stats(es=None):
    es = es or get_connection()
    now = datetime.utcnow().isoformat()

    rstc = defaultdict(lambda: defaultdict(int))

    sql = """SELECT parent,type,count(id)
             FROM uuids
             WHERE deleted=false and (type='record' or type='mediarecord')
             GROUP BY parent,type"""

    for parent, type, count in apidbpool.fetchiter(sql):
        rstc[parent][type + "s_count"] = count

    rstc = dict(rstc)

    for k, rsc in rstc.items():
        rsc.setdefault("records_count", 0)
        rsc.setdefault("mediarecords_count", 0)
        rsc["harvest_date"] = now
        rsc["recordset_id"] = k

        es.index(index=indexName, doc_type="api", body=rsc)
