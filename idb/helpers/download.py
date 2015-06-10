from config import config
import elasticsearch
import elasticsearch.helpers

import unicodecsv as csv
import json
import traceback
import zipfile
import os

from conversions import index_field_to_longname
from query_shim import queryFromShim
from meta_xml import make_meta, make_file_block
from identification import identifiy_locality, identifiy_scientificname
from collections import Counter

from cStringIO import StringIO, OutputType

import logging

# sl = config["elasticsearch"]["servers"]
sl = [
    "c17node52.acis.ufl.edu",
    "c17node53.acis.ufl.edu",
    "c17node54.acis.ufl.edu",
    "c17node55.acis.ufl.edu",
    "c17node56.acis.ufl.edu"
]

es = elasticsearch.Elasticsearch(
    sl, sniff_on_start=True, sniff_on_connection_fail=True, retry_on_timeout=True, max_retries=10)
indexName = "idigbio-2.3.0"

use_string_io = False


def count_query(t, query):
    return es.count(index=indexName, doc_type=t, body=query)["count"]


def query_to_uniquevals(outf, t, body, val_field, tabs, val_func):
    cw = None
    if tabs:
        cw = csv.writer(outf, dialect=csv.excel_tab)
    else:
        cw = csv.writer(outf)

    cw.writerow(
        ["id", index_field_to_longname[t][val_field], "idigbio:itemCount"])

    values = Counter()
    for r in elasticsearch.helpers.scan(es, query=body, size=1000, doc_type=t):
        try:
            v = r["_source"][val_field]
            if val_field == "scientificname":
                v = v.capitalize()
            values[v] += 1
        except:
            traceback.print_exc()

    for k, v in values.most_common():
        cw.writerow([val_func(k), k, v])


def query_to_csv(outf, t, body, header_fields, fields, id_field, raw, tabs, id_func):
    cw = None
    if tabs:
        cw = csv.writer(outf, dialect=csv.excel_tab)
    else:
        cw = csv.writer(outf)

    cw.writerow([id_field] + header_fields)

    for r in elasticsearch.helpers.scan(es, query=body, size=1000, doc_type=t):
        try:
            rec_root = r["_source"]
            if raw:
                if "data" in r["_source"]:
                    rec_root = r["_source"]["data"]
                else:
                    rec_root = []

            r_fields = [id_func(r)]
            fa = fields
            if raw:
                fa = ["".join(f[5:]) for f in fields]
            for k in fa:
                if k in rec_root:
                    if isinstance(rec_root[k], str) or isinstance(rec_root[k], unicode):
                        r_fields.append(rec_root[k])
                    else:
                        r_fields.append(
                            json.dumps(rec_root[k]))
                else:
                    r_fields.append("")
            cw.writerow(r_fields)
        except:
            traceback.print_exc()


def acceptable_field_name(f):
    return "\"" not in f and " " not in f


def make_file(t, query, raw=False, tabs=False, fields=None, core=True, id_func=lambda r: r["_id"], file_prefix="", final_filename=""):
    file_extension = ".csv"
    if tabs:
        file_extension = ".tsv"

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

        mapping_root = mapping[indexName]["mappings"][t]["properties"]
        if raw:
            mapping_root = mapping[indexName]["mappings"][
                t]["properties"]["data"]["properties"]

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
            converted_fields = ["".join(f[5:]) for f in fields]
        else:
            converted_fields = [index_field_to_longname[t][f] for f in fields]

        meta_block = make_file_block(
            filename=final_filename + file_extension, core=core, tabs=tabs, fields=converted_fields, t=t)

        fields_include = fields
        fields_exclude = ["data"]
        if raw:
            fields_include = ["records", "mediarecords"] + fields
            fields_exclude = []

        body = {
            "_source": {
                "include": fields_include,
                "exclude": fields_exclude
            },
            "query": query
        }

        if use_string_io:
            sio = StringIO()
            query_to_csv(
                sio, t, body, converted_fields, fields, id_field, raw, tabs, id_func)
            sio.seek(0)
            return (sio, final_filename + file_extension, meta_block)
        else:
            with open(outfile_name, "wb") as outf:
                query_to_csv(
                    outf, t, body, converted_fields, fields, id_field, raw, tabs, id_func)
            return (outfile_name, final_filename + file_extension, meta_block)
    elif t == "uniquelocality":
        body = {
            "_source": ["locality"],
            "query": query
        }

        if use_string_io:
            sio = StringIO()
            query_to_uniquevals(
                sio, "records", body, "locality", tabs, identifiy_locality)
            sio.seek(0)
            return (sio, final_filename + file_extension, "")
        else:
            with open(outfile_name, "wb") as outf:
                query_to_uniquevals(
                    outf, "records", body, "locality", tabs, identifiy_locality)
            return (outfile_name, final_filename + file_extension, "")

    elif t == "uniquenames":
        body = {
            "_source": ["scientificname"],
            "query": query
        }

        if use_string_io:
            sio = StringIO()
            query_to_uniquevals(
                sio, "records", body, "scientificname", tabs, identifiy_scientificname)
            sio.seek(0)
            return (sio, final_filename + file_extension, "")
        else:
            with open(outfile_name, "wb") as outf:
                query_to_uniquevals(
                    outf, "records", body, "scientificname", tabs, identifiy_scientificname)
            return (outfile_name, final_filename + file_extension, "")


def generate_files(core_type="records", core_source="indexterms", record_query=None, mediarecord_query=None,
                   form="csv", filename="dump", record_fields=None, mediarecord_fields=None):

    rq_and = []
    mq_and = []

    if record_query is not None:
        if "filtered" in record_query:
            rq_and.extend(record_query["filtered"]["filter"]["and"])
        mq_and.append({
            "has_parent": {
                "parent_type": "records",
                "query": record_query,
                "inner_hits": {
                    "_source": ["uuid", "scientificname", "locality"]
                }
            }
        })
        rq = record_query
    else:
        rq = {
            "filtered": {
                "filter": {
                    "and": [
                        {
                            "match_all": {}
                        }
                    ]
                }
            }
        }

    if mediarecord_query is not None:
        if "filtered" in mediarecord_query:
            mq_and.extend(record_query["filtered"]["filter"]["and"])
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
                        {
                            "match_all": {}
                        }
                    ]
                }
            }
        }

    rq["filtered"]["filter"]["and"] = rq_and
    mq["filtered"]["filter"]["and"] = mq_and

    if form in ["csv", "tsv"]:
        q = None
        tabs = form == "tsv"
        fields = None

        if core_type == "records":
            q = rq
            fields = record_fields
        elif core_type == "mediarecords":
            q = mq
            fields = mediarecord_fields

        if core_type in ["records", "mediarecords"]:
            if core_source == "indexterms":
                return make_file(core_type, q, tabs=tabs, core=True, file_prefix=filename + ".", fields=fields)
            elif core_source == "raw":
                return make_file(core_type, q, raw=True, tabs=tabs, core=True, file_prefix=filename + ".", fields=fields)
        elif core_type == "uniquenames":
            rq["filtered"]["filter"]["and"].append({
                "exists": {
                    "field": "scientificname"
                }
            })
            return make_file(core_type, rq, tabs=tabs, core=True, file_prefix=filename + ".", fields=fields)
        elif core_type == "uniquelocality":
            rq["filtered"]["filter"]["and"].append({
                "exists": {
                    "field": "locality"
                }
            })            
            return make_file(core_type, rq, tabs=tabs, core=True, file_prefix=filename + ".", fields=fields)

    elif form.startswith("dwca"):
        tabs = False
        internal_form = form.split("-")
        if len(internal_form) > 1 and internal_form[1] == "tsv":
            tabs = True

        rec_ind_fields = None
        rec_raw_fields = None
        if record_fields is not None:
            rec_ind_fields = []
            rec_raw_fields = []
            for f in record_fields:
                if f.startswith("data."):
                    rec_raw_fields.append(f)
                else:
                    rec_ind_fields.append(f)

        med_ind_fields = None
        med_raw_fields = None
        if mediarecord_fields is not None:
            med_ind_fields = []
            med_raw_fields = []
            for f in mediarecord_fields:
                if f.startswith("data."):
                    med_raw_fields.append(f)
                else:
                    med_ind_fields.append(f)

        rec_core = core_type == "records"
        ind_core = core_source == "indexterms"

        rec_ind_core = rec_core and ind_core
        rec_raw_core = rec_core and not ind_core
        med_ind_core = not rec_core and ind_core
        med_raw_core = not rec_core and not ind_core

        # Bugs in id/coreid generation
        files = [

            make_file("records", rq, tabs=tabs, core=rec_ind_core,
                      file_prefix=filename + ".", fields=rec_ind_fields, final_filename="occurrence"),
            make_file(
                "records", rq, raw=True, tabs=tabs, core=rec_raw_core, file_prefix=filename + ".", fields=rec_raw_fields, final_filename="occurrence_raw"),
            make_file(
                "mediarecords", mq, tabs=tabs, core=med_ind_core, id_func=lambda r: r["_source"]["records"][0], file_prefix=filename + ".", fields=med_ind_fields, final_filename="multimedia",),
            make_file("mediarecords", mq, raw=True, tabs=tabs, core=med_raw_core, id_func=lambda r: r[
                "_source"]["records"][0], file_prefix=filename + ".", fields=med_raw_fields, final_filename="multimedia_raw"),
        ]

        meta_string = None
        with zipfile.ZipFile(filename + ".zip", 'w', zipfile.ZIP_DEFLATED, True) as expzip:
            meta_files = []
            for f in files:
                if f[0] is not None:
                    if isinstance(f[0], OutputType):
                        expzip.writestr(f[1], f[0].read())
                    else:
                        expzip.write(f[0], f[1])
                        os.unlink(f[0])
                    meta_files.append(f[2])
            meta_string = make_meta(meta_files)
            expzip.writestr("meta.xml", meta_string)

        return (filename + ".zip", filename + ".zip", meta_string)


def main():
    import itertools
    import datetime
    import uuid

    logger = logging.getLogger()
    logger.setLevel(logging.ERROR)

    # indexName = "idigbio-" + config["elasticsearch"]["indexname"]

    # run_id = str(uuid.uuid4())

    rq = {"genus": "acer"}

    record_query = queryFromShim(rq, "records")["query"]

    mediarecord_query = None

    core_types = ["records", "mediarecords"]
    core_sources = ["indexterms", "raw"]
    forms = ["csv", "tsv", "dwca-csv", "dwca-tsv"]

    combos = itertools.product(core_types, core_sources, forms)

    # core_types = ["uniquenames", "uniquelocality"]
    # core_sources = ["indexterms"]
    # forms = ["csv", "tsv"]

    # combos = itertools.product(core_types, core_sources, forms)

    for t, s, f in combos:
        try:
            print t, s, f, generate_files(core_type=t, core_source=s, form=f, record_query=record_query, mediarecord_query=mediarecord_query, filename=str(uuid.uuid4()))[0]
        except:
            traceback.print_exc()
            print

    # print generate_files(core_type="mediarecords", core_source="indexterms",
    # record_query=record_query, mediarecord_query=None, form="csv",
    # filename=str(uuid.uuid4()))

    # print generate_files(core_type="records", core_source="indexterms",
    # record_query=record_query, mediarecord_query=None, form="dwca",
    # filename=str(uuid.uuid4()))
    # print generate_files(core_type="records", core_source="indexterms",
    #                      record_query=record_query, mediarecord_query=None, form="dwca-csv", filename=str(uuid.uuid4()))
    # print generate_files(core_type="records", core_source="indexterms",
    # record_query=record_query, mediarecord_query=None, form="dwca-tsv",
    # filename=str(uuid.uuid4()))

    # load testing

    # query_components = [
    #     {"family": "asteraceae"},
    #     {"hasImage": True},
    #     {"data": {"type": "fulltext", "value": "aster"}},
    #     {"scientificname": {"type": "exists"}},
    #     {"minelevation": {"type": "range", "gte": "100", "lte": "200"}},
    #     {"geopoint": {"type": "geo_bounding_box", "top_left": {
    #         "lat": 19.23, "lon": -130}, "bottom_right": {"lat": -45.1119, "lon": 179.99999}}},
    # ]
    # queries = []
    # for c in range(0, len(query_components) + 1):
    #     a = itertools.combinations(query_components, c)
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
    #                                         form="csv", filename=str(uuid.uuid4()))) + "\"", "geopoint" in qt[1], (datetime.datetime.now() - t).total_seconds()
    #         except:
    #             print qt
    #             traceback.print_exc()

if __name__ == '__main__':
    main()
