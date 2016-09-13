from __future__ import division, absolute_import, print_function

import json
import logging
import zipfile
import os
import datetime

from collections import Counter

import elasticsearch
import elasticsearch.helpers
import unicodecsv as csv

from atomicfile import AtomicFile

# idb imports
from idb.helpers.conversions import index_field_to_longname
from idb.indexing.indexer import get_connection, get_indexname

# local imports
from .query_shim import queryFromShim
from .meta_xml import make_meta, make_file_block
from .identification import identifiy_locality, identifiy_scientificname

logger = logging.getLogger()

indexName = get_indexname()
es = get_connection()

# 0: Current Year
# 1: Query Text
# 2: Total Number of Records
# 3: Access Datetime
# 4: Number of recordsets
# 5: List of recordset IDs and counts
citation_format = """http://www.idigbio.org/portal ({0}),
Query: {1},
{2} records, accessed on {3},
contributed by {4} Recordsets, Recordset identifiers:
{5}"""

def write_citation_file(dl_id, t, query, recordsets):
    filename = "{0}.{1}.citation.txt".format(dl_id, t)
    with AtomicFile(filename, "wb") as citefile:
        rs_string = ""
        total_recs = 0
        total_rs = len(recordsets.keys())
        for rs, rsc in sorted([(rs, recordsets[rs]) for rs in recordsets], key=lambda x: x[1], reverse=True):
            rs_string += "http://www.idigbio.org/portal/recordsets/{0} ({1} records)\n".format(rs, rsc)
            total_recs += rsc

        query_string = json.dumps(query)

        now = datetime.datetime.now()

        # 0: Current Year
        # 1: Query Text
        # 2: Total Number of Records
        # 3: Access Datetime
        # 4: Number of recordsets
        # 5: List of recordset IDs and counts
        citefile.write(citation_format.format(
            now.year,           # 0: Current Year
            query_string,  # 1: Query Text
            total_recs,         # 2: Total Number of Records
            now.isoformat(),    # 3: Access Datetime
            total_rs,           # 4: Number of recordsets
            rs_string,          # 5: List of recordset IDs and counts
        ))
        if total_recs > 0:
            return filename


def get_recordsets(params, generate=True):
    rq, mq = None, None

    if generate:
        record_query = None
        mediarecord_query = None
        if params["rq"] is not None:
            record_query = queryFromShim(params["rq"])["query"]

        if params["mq"] is not None:
            mediarecord_query = queryFromShim(params["mq"])["query"]
        rq, mq = generate_queries(record_query, mediarecord_query)
    else:
        rq = params["rq"]
        mq = params["mq"]

    q = None
    t = None
    if params["core_type"] == "mediarecords":
        t = "mediarecords"
        q = {
            "query": mq,
            "aggs": {
                "recordsets": {
                    "terms": {
                        "field": "recordset",
                        "size": 1000
                    }
                }
            }
        }
    else:
        t = "records"
        q = {
            "query": rq,
            "aggs": {
                "recordsets": {
                    "terms": {
                        "field": "recordset",
                        "size": 1000
                    }
                }
            }
        }

    ro = es.search(index=indexName, doc_type=t, body=q)
    recsets = {}
    for b in ro["aggregations"]["recordsets"]["buckets"]:
        recsets[b["key"]] = b["doc_count"]
    return (q, recsets)


def write_citation_files(dl_id, rq, mq, record_query, mediarecord_query):
    for t in ["records", "mediarecords"]:
        query = None
        if t == "records":
            query = record_query
        elif t == "mediarecords":
            query = mediarecord_query
        cf = write_citation_file(dl_id, t, query, get_recordsets({
            "rq": rq,
            "mq": mq,
            "core_type": t
        }, generate=False)[1])
        if cf is not None:
            yield cf


def count_query(t, query):
    return es.count(index=indexName, doc_type=t, body=query)["count"]


def get_source_value(source, val_field):
    local_source = source
    for vf in val_field.split("."):
        if vf in local_source:
            local_source = local_source[vf]
        else:
            return None
    else:
        return local_source


def query_to_uniquevals(outf, t, body, val_field, tabs, val_func):
    cw = None
    if tabs:
        cw = csv.writer(outf, dialect=csv.excel_tab)
    else:
        cw = csv.writer(outf)

    ifn = None
    if val_field.startswith("data."):
        ifn = val_field.split(".")[-1]
    else:
        ifn = index_field_to_longname[t][val_field]

    cw.writerow(
        ["id", ifn, "idigbio:itemCount"])

    values = Counter()
    for r in elasticsearch.helpers.scan(es, index=indexName, query=body, size=1000, doc_type=t):
        source = get_source_value(r["_source"],val_field)
        try:
            if source is not None:
                v = source
                if val_field.lower().endswith("scientificname"):
                    v = v.capitalize()
                values[v] += 1
            else:
                values[""] += 1
        except:
            logger.exception("Error generating uniquevals")

    for k, v in values.most_common():
        cw.writerow([val_func(k), k, v])


def query_to_csv(outf, t, body, header_fields, fields, id_field, raw, tabs, id_func):
    cw = None
    if tabs:
        cw = csv.writer(outf, dialect=csv.excel_tab)
    else:
        cw = csv.writer(outf)

    cw.writerow([id_field] + header_fields)

    for r in elasticsearch.helpers.scan(es, index=indexName, query=body, size=1000, doc_type=t):
        try:
            r_fields = [id_func(r)]
            for k in fields:
                v = get_source_value(r["_source"],k)
                if v is not None:
                    if isinstance(v, str) or isinstance(v, unicode):
                        r_fields.append(v)
                    else:
                        r_fields.append(json.dumps(v))
                else:
                    r_fields.append("")
            cw.writerow(r_fields)
        except:
            logger.exception("Error generating csv")


def acceptable_field_name(f):
    return "\"" not in f and " " not in f


type_core_type_ids = {
    # Core Type, File Type, Core Source : id_func, id_field
    ("records", "records", "indexterms"): (lambda r: r["_id"], None),
    ("records", "mediarecords", "indexterms"): (lambda r: r["_source"]["records"][0] if "records" in r["_source"] else "", "records"),
    ("records", "records", "raw"): (lambda r: r["_id"], None),
    ("records", "mediarecords", "raw"): (lambda r: r["_source"]["records"][0], "records"),
    ("mediarecords", "mediarecords", "indexterms"): (lambda r: r["_id"], None),
    ("mediarecords", "records", "indexterms"): (lambda r: r["_source"]["mediarecords"][0], "mediarecords"),
    ("mediarecords", "mediarecords", "raw"): (lambda r: r["_id"], None),
    ("mediarecords", "records", "raw"): (lambda r: r["_source"]["mediarecords"][0], "mediarecords"),
    ("uniquelocality", "uniquelocality", "indexterms"): (identifiy_locality, "locality"),
    ("uniquelocality", "records", "indexterms"): (lambda r: identifiy_locality(get_source_value(r["_source"],"locality")),"locality"),
    ("uniquelocality", "mediarecords", "indexterms"): (lambda r: identifiy_locality(get_source_value(r["inner_hits"]["records"]["hits"]["hits"][0]["_source"],"locality")),"locality"),
    ("uniquelocality", "uniquelocality", "raw"): (identifiy_locality, "data.dwc:locality"),
    ("uniquelocality", "records", "raw"): (lambda r: identifiy_locality(get_source_value(r["_source"],"data.dwc:locality")),"data.dwc:locality"),
    ("uniquelocality", "mediarecords", "raw"): (lambda r: identifiy_locality(get_source_value(r["inner_hits"]["records"]["hits"]["hits"][0]["_source"],"data.dwc:locality")),"data.dwc:locality"),
    ("uniquenames", "uniquenames", "indexterms"): (identifiy_scientificname, "scientificname"),
    ("uniquenames", "records", "indexterms"): (lambda r: identifiy_scientificname(get_source_value(r["_source"],"scientificname")),"scientificname"),
    ("uniquenames", "mediarecords", "indexterms"): (lambda r: identifiy_locality(get_source_value(r["inner_hits"]["records"]["hits"]["hits"][0]["_source"],"scientificname")),"scientificname"),
    ("uniquenames", "uniquenames", "raw"): (identifiy_scientificname, "data.dwc:scientificName"),
    ("uniquenames", "records", "raw"): (lambda r: identifiy_scientificname(get_source_value(r["_source"],"data.dwc:scientificName")),"data.dwc:scientificName"),
    ("uniquenames", "mediarecords", "raw"): (lambda r: identifiy_locality(get_source_value(r["inner_hits"]["records"]["hits"]["hits"][0]["_source"],"data.dwc:scientificName")),"data.dwc:scientificName"),
}

def make_file(t, query, raw=False, tabs=False, fields=None,
              core_type="records", core_source="indexterms", file_prefix="", final_filename=""):
    file_extension = ".tsv" if tabs else ".csv"

    core = t == core_type and raw == core_source == "raw"

    id_func, core_id_field = type_core_type_ids[(core_type,t,core_source)]

    outfile_name = file_prefix + t + file_extension
    if raw:
        outfile_name = file_prefix + t + ".raw" + file_extension

    if t in ["records", "mediarecords"]:
        id_field = "id"
        if not core:
            id_field = "coreid"

        exclude_from_fields = ["data"]
        if raw:
            exclude_from_fields = ["id", "coreid"]

        mapping = es.indices.get_mapping(index=indexName, doc_type=t)
        mapping_root = mapping.values()[0]["mappings"][t]["properties"]
        if raw:
            mapping_root = mapping_root["data"]["properties"]

        if fields is None:
            fields = []
            for f in mapping_root:
                if f not in exclude_from_fields and acceptable_field_name(f):
                    if raw:
                        fields.append("data." + f)
                    else:
                        fields.append(f)
            fields = sorted(fields)
        elif len(fields) == 0:
            return (None, None, None)

        if raw:
            # Remove "data."
            converted_fields = ["".join(f[5:]) for f in fields]
        else:
            converted_fields = []
            filtered_fields = []
            for f in fields:
                if f in index_field_to_longname[t]:
                    converted_fields.append(index_field_to_longname[t][f])
                    filtered_fields.append(f)
            fields = filtered_fields

        meta_block = make_file_block(
            filename=final_filename + file_extension, core=core, tabs=tabs, fields=converted_fields, t=t)

        if core_id_field is not None:
            fields_include = fields + [core_id_field]
        else:
            fields_include = fields

        body = {
            "_source": fields_include,
            "query": query
        }

        with AtomicFile(outfile_name, "wb") as outf:
            query_to_csv(
                outf, t, body, converted_fields, fields, id_field, raw, tabs, id_func)
        return (outfile_name, final_filename + file_extension, meta_block)
    elif t.startswith("unique"):
        if t == "uniquelocality":
            unique_field = "locality"
            if raw:
                unique_field = "data.dwc:locality"
        elif t == "uniquenames":
            unique_field = "scientificname"
            if raw:
                unique_field = "data.dwc:scientificName"

        body = {
            "_source": [unique_field],
            "query": query
        }

        converted_fields = None
        if unique_field.startswith("data."):
            converted_fields = [unique_field[5:], "idigbio:itemCount"]
        else:
            converted_fields = [index_field_to_longname["records"][unique_field], "idigbio:itemCount"]

        meta_block = make_file_block(
            filename=final_filename + file_extension, core=core, tabs=tabs, fields=converted_fields, t=t)

        with AtomicFile(outfile_name, "wb") as outf:
            query_to_uniquevals(
                outf, "records", body, unique_field, tabs, identifiy_locality)
        return (outfile_name, final_filename + file_extension, meta_block)


def generate_queries(record_query=None, mediarecord_query=None, core_type=None):
    rq = None
    mq = None

    rq_and = []
    mq_and = []

    if core_type == "mediarecords":
        rq_and.append({
            "term": {
                "hasImage": True
            }
        })

    if record_query == {"filtered":{"filter":{}}}:
        record_query = None
    if mediarecord_query == {"filtered":{"filter":{}}}:
        mediarecord_query = None

    if record_query is not None:
        if "and" in record_query["filtered"]["filter"]:
            rq_and.extend(record_query["filtered"]["filter"]["and"])
        mq_and.append({
            "has_parent": {
                "parent_type": "records",
                "query": record_query,
                "inner_hits": {
                    "_source": ["scientificname", "locality", "data.dwc:scientificName", "data.dwc:locality"]
                }
            }
        })
        rq = record_query
    else:
        rq = {
            "filtered": {
                "filter": {
                    "and": [
                    ]
                }
            }
        }
        rq_and.append({
            "match_all": {}
        })

    if mediarecord_query is not None:
        if "and" in mediarecord_query["filtered"]["filter"]:
            mq_and.extend(mediarecord_query["filtered"]["filter"]["and"])
        rq_and.append({
            "has_child": {
                "child_type": "mediarecords",
                "query": mediarecord_query
            }
        })
        mq = mediarecord_query
    else:
        mq = {
            "filtered": {
                "filter": {
                    "and": [
                    ]
                }
            }
        }
        mq_and.append({
            "match_all": {}
        })

    rq["filtered"]["filter"]["and"] = rq_and
    mq["filtered"]["filter"]["and"] = mq_and

    return (rq, mq)


def generate_files(core_type="records", core_source="indexterms", record_query=None, mediarecord_query=None,
                   form="csv", filename="dump", record_fields=None, mediarecord_fields=None):

    if form in ["csv", "tsv"]:
        rq, mq = generate_queries(record_query, mediarecord_query, core_type)

        q = None
        tabs = form == "tsv"
        fields = None

        if core_type == "records":
            q = rq
            fields = record_fields
        elif core_type == "mediarecords":
            q = mq
            fields = mediarecord_fields
        elif core_type.startswith("unique"):
            q = rq

        return make_file(core_type, q, raw=(core_source == "raw"), tabs=tabs, core_type=core_type, core_source=core_source, file_prefix=filename + ".", fields=fields)[0]

    elif form.startswith("dwca"):
        meta_string = None
        files = generate_dwca_files(
            core_type=core_type, core_source=core_source,
            record_query=record_query, mediarecord_query=mediarecord_query,
            form=form, filename=filename,
            record_fields=record_fields, mediarecord_fields=mediarecord_fields)
        zipfilename = filename + ".zip"
        with zipfile.ZipFile(zipfilename, 'w', zipfile.ZIP_DEFLATED, True) as expzip:
            meta_files = []
            for f in files:
                if f[0] is not None:
                    expzip.write(f[0], f[1])
                    os.unlink(f[0])
                    if f[2] is not None:
                        meta_files.append(f[2])
            meta_string = make_meta(meta_files)
            expzip.writestr("meta.xml", meta_string)
        return zipfilename


def generate_dwca_files(core_type="records", core_source="indexterms", record_query=None, mediarecord_query=None,
                        form="dwca", filename="dump", record_fields=None, mediarecord_fields=None):

    tabs = form.endswith("tsv")

    rq, mq = generate_queries(record_query, mediarecord_query, core_type)
    type_source_options = {
        ("records", "indexterms"): (
            [
                "records",
                rq
            ],
            {
                "raw": False,
                "tabs": tabs,
                "core_type": core_type,
                "core_source": core_source,
                "file_prefix": filename + ".",
                "fields": None,
                "final_filename": "occurrence"
            }
        ),
        ("records", "raw"): (
            [
                "records",
                rq
            ],
            {
                "raw": True,
                "tabs": tabs,
                "core_type": core_type,
                "core_source": core_source,
                "file_prefix": filename + ".",
                "fields": None,
                "final_filename": "occurrence_raw"
            }
        ),
        ("mediarecords", "indexterms"): (
            [
                "mediarecords",
                mq
            ],
            {
                "raw": False,
                "tabs": tabs,
                "core_type": core_type,
                "core_source": core_source,
                "file_prefix": filename + ".",
                "fields": None,
                "final_filename": "multimedia"
            }
        ),
        ("mediarecords", "raw"): (
            [
                "mediarecords",
                mq
            ],
            {
                "raw": True,
                "tabs": tabs,
                "core_type": core_type,
                "core_source": core_source,
                "file_prefix": filename + ".",
                "fields": None,
                "final_filename": "multimedia_raw"
            }
        ),
    }

    if record_fields is not None:
        type_source_options[("records","raw")][1]["fields"] = []
        type_source_options[("records","indexterms")][1]["fields"] = []
        for f in record_fields:
            if f.startswith("\""):
                continue
            elif f.startswith("data."):
                type_source_options[("records", "raw")][1]["fields"].append(f)
            else:
                type_source_options[("records", "indexterms")][1]["fields"].append(f)

    if mediarecord_fields is not None:
        type_source_options[("mediarecords", "raw")][1]["fields"] = []
        type_source_options[("mediarecords", "indexterms")][1]["fields"] = []
        for f in mediarecord_fields:
            if f.startswith("\""):
                continue
            elif f.startswith("data."):
                type_source_options[("mediarecords","raw")][1]["fields"].append(f)
            else:
                type_source_options[("mediarecords","indexterms")][1]["fields"].append(f)

    # Order is important here, core must be first for correct meta.xml generation
    if core_type == "uniquelocality":
        yield make_file(
            core_type, rq, raw=(core_source == "raw"), tabs=tabs, core_type=core_type, core_source=core_source,
            file_prefix=filename + ".", fields=None, final_filename="locality"
        )
    elif core_type == "uniquenames":
        yield make_file(
            core_type, rq, raw=(core_source == "raw"), tabs=tabs, core_type=core_type, core_source=core_source,
            file_prefix=filename + ".", fields=None, final_filename="names"
        )
    else:
        # Write out core
        args, kwargs = type_source_options.pop((core_type, core_source))
        yield make_file(*args, **kwargs)

    #TODO: just use `.values`
    for t,s in type_source_options:
        # Write out extensions
        args, kwargs = type_source_options[(t,s)]
        yield make_file(*args, **kwargs)

    for f in write_citation_files(filename, rq, mq, record_query, mediarecord_query):
        yield (f, f.split(".", 1)[-1], None)

def main():
    import itertools
    import datetime
    import uuid

    logger = logging.getLogger()
    logger.setLevel(logging.ERROR)

    # run_id = str(uuid.uuid4())
    # Form Testing
    rq = {"genus": "acer", "stateprovince": "florida"}

    record_query = queryFromShim(rq, "records")["query"]

    mediarecord_query = None

    print(generate_files(core_type="records", core_source="indexterms", form="dwca-csv", record_query=record_query, mediarecord_query=mediarecord_query, filename=str(uuid.uuid4())))

    # core_types = ["records", "mediarecords", "uniquelocality", "uniquenames"]
    # core_sources = ["indexterms", "raw"]
    # forms = ["csv", "tsv", "dwca-csv", "dwca-tsv"]

    # combos = itertools.product(core_types, core_sources, forms)

    # for t, s, f in combos:
    #     try:
    #         print t, s, f, generate_files(core_type=t, core_source=s, form=f, record_query=record_query, mediarecord_query=mediarecord_query, filename=str(uuid.uuid4()))
    #     except:
    #         traceback.print_exc()
    #         print

    # load testing

    # record_query_components = [
    #     {"family": "asteraceae"},
    #     {"hasImage": True},
    #     {"data": {"type": "fulltext", "value": "aster"}},
    #     {"scientificname": {"type": "exists"}},
    #     {"minelevation": {"type": "range", "gte": "100", "lte": "200"}},
    #     {"geopoint": {"type": "geo_bounding_box", "top_left": {
    #         "lat": 19.23, "lon": -130}, "bottom_right": {"lat": -45.1119, "lon": 179.99999}}},
    # ]
    # # mediarecord_query_components = [
    # #     {""}
    # # ]
    # queries = []
    # for c in range(0, len(record_query_components) + 1):
    #     a = itertools.combinations(record_query_components, c)
    #     for qcs in a:
    #         q = {}
    #         for qc in qcs:
    #             q.update(qc)

    #         record_query = queryFromShim(q)
    #         qc = count_query("records", record_query)
    #         queries.append((qc, q))

    # for qt in sorted(queries, key=lambda x: x[0]):
    #     record_query = queryFromShim(qt[1])
    #     mediarecord_query = None
    #     if qt[0] < 100000000:
    #         try:
    #             t = datetime.datetime.now()
    #             print qt[0], "\"" + repr(generate_files(core_type="records", core_source="indexterms",
    #                                         record_query=record_query[
    #                                             "query"], mediarecord_query=None,
    #                                         form="dwca-csv", filename=str(uuid.uuid4()))) + "\"", "geopoint" in qt[1], (datetime.datetime.now() - t).total_seconds()
    #         except:
    #             print qt
    #             traceback.print_exc()

if __name__ == '__main__':
    main()
