import sys
import traceback
from idb.helpers.logging import getLogger, configure_app_log

configure_app_log(2, journal='auto')
logger = getLogger("stats-rewrite")

import datetime
import time

from elasticsearch.helpers import expand_action, scan, bulk
from elasticsearch.exceptions import ConnectionTimeout, ConnectionError
from elasticsearch import Elasticsearch
from collections import Counter
import json


from elasticsearch.helpers import reindex

def resume_reindex(client, source_index, target_index, query=None, target_client=None,
        chunk_size=500, scroll='5m', scan_kwargs={}, bulk_kwargs={}):

    """
    Reindex all documents from one index that satisfy a given query
    to another, potentially (if `target_client` is specified) on a different cluster.
    If you don't specify the query you will reindex all the documents.
    .. note::
        This helper doesn't transfer mappings, just the data.
    :arg client: instance of :class:`~elasticsearch.Elasticsearch` to use (for
        read if `target_client` is specified as well)
    :arg source_index: index (or list of indices) to read documents from
    :arg target_index: name of the index in the target cluster to populate
    :arg query: body for the :meth:`~elasticsearch.Elasticsearch.search` api
    :arg target_client: optional, is specified will be used for writing (thus
        enabling reindex between clusters)
    :arg chunk_size: number of docs in one chunk sent to es (default: 500)
    :arg scroll: Specify how long a consistent view of the index should be
        maintained for scrolled search
    :arg scan_kwargs: additional kwargs to be passed to
        :func:`~elasticsearch.helpers.scan`
    :arg bulk_kwargs: additional kwargs to be passed to
        :func:`~elasticsearch.helpers.bulk`
    """
    target_client = client if target_client is None else target_client

    logger.info("Building skip list")
    target_docs = set()
    for h in scan(
        target_client,
        query={
          "query": query["query"],
          "fields": [
                "_id"
           ]
        }, index=target_index,
        scroll=scroll,
        size=5000
    ):
        target_docs.add(h['_id'])
    logger.info("Skip list build: {} docs".format(len(target_docs)))    

    base_num = 0
    end_num = 48

    try:
        with open("numbers.txt","r") as inf:
            base_num, end_num = inf.read().split(",")
    except Exception:
        pass
    logger.info("Start: {}, End: {}".format(base_num, end_num))

    base_query = query["query"]
    # A successful index returns out of this function.

    #dates = [datetime.date(*(2017-int(i/12),12 - i%12, 1)) for i in range(base_num, end_num)]
    base = datetime.datetime.today()
    dates = [base - datetime.timedelta(days=x*30) for x in range(base_num, end_num)]
    for i, d in enumerate(dates[:-1]):
        try:
            query = {
                "query": {
                    "bool": {
                        "must": [
                            base_query,
                            {
                                "range": {
                                    "harvest_date": {
                                        "lte": d.isoformat(),
                                        "gte": dates[i+1].isoformat()
                                    }
                                }
                            }
                        ]
                    }
                }
            }
            logger.debug(", ".join([str(base_num + i), json.dumps(query)]))
            docs = scan(client, query=query, index=source_index, scroll=scroll, **scan_kwargs)
            stats = Counter()
            # def _change_doc_index(hits, index):
            #     for h in hits:
            #         stats["docs"] += 1
            #         if h["_id"] in target_docs:
            #             stats["skip"] += 1
            #         else:
            #             h['_index'] = index
            #             stats["done"] += 1

            #             yield h
            #             target_docs.add(h["_id"])

            #         if stats["docs"] % 10000 == 0:
            #             print stats.most_common()


            # bulk(target_client, _change_doc_index(docs, target_index),
            #     chunk_size=chunk_size, stats_only=True, **bulk_kwargs)

            for h in docs:
                stats["docs"] += 1
                if h["_id"] in target_docs:
                    stats["skip"] += 1
                else:
                    print h
                    target_client.index(index=target_index,doc_type=h["_type"],body=h["_source"])                    
                    stats["done"] += 1
                    target_docs.add(h["_id"])

                if stats["docs"] % 10000 == 0:
                    print stats.most_common()                    


            logger.info(stats.most_common())
            logger.info(base_num + i, "Done")
            try:
                with open("numbers.txt","w") as inf:
                    inf.write("{},{}".format(base_num+1,end_num))
            except Exception:
                logger.excetion("Error writing skip file.")
                pass
        except (ConnectionTimeout,) as e:
            logger.exception("Timeout or Error in {}, continuing in 10 seconds".format(d))
            time.sleep(10)



es = Elasticsearch([
    "c18node2.acis.ufl.edu",
    "c18node6.acis.ufl.edu",
    "c18node10.acis.ufl.edu",
    "c18node12.acis.ufl.edu",
    "c18node14.acis.ufl.edu"
], sniff_on_start=False, sniff_on_connection_fail=False,retry_on_timeout=False,max_retries=3,timeout=30)

# esb = Elasticsearch([
#     "c17node52.acis.ufl.edu",
#     "c17node53.acis.ufl.edu",
#     "c17node54.acis.ufl.edu",
#     "c17node55.acis.ufl.edu",
#     "c17node56.acis.ufl.edu"
# ], sniff_on_start=True, sniff_on_connection_fail=True,retry_on_timeout=True,max_retries=3)

# es = Elasticsearch([
#     'c17node52.acis.ufl.edu:9200',
#     'c17node53.acis.ufl.edu:9200',
#     'c17node54.acis.ufl.edu:9200',
#     'c17node55.acis.ufl.edu:9200',
#     'c17node56.acis.ufl.edu:9200'
# ], sniff_on_start=True, sniff_on_connection_fail=True,retry_on_timeout=True,max_retries=3)


# 1.0.0 -> 2.0.0
# m = es.indices.get_mapping(index="stats",doc_type="api,digest,search,fields")

# rs_map = m["stats"]["mappings"]["search"]["properties"]["recordset"]
# d_map = m["stats"]["mappings"]["search"]["properties"]["date"]
# fd_map = m["stats"]["mappings"]["fields"]["properties"]["date"]

# del m["stats"]["mappings"]["search"]["properties"]["recordset"]
# del m["stats"]["mappings"]["search"]["properties"]["date"]
# del m["stats"]["mappings"]["fields"]["properties"]["date"]

# rs_map["analyzer"] = "keyword"

# m["stats"]["mappings"]["search"]["properties"]["recordset_id"] = rs_map
# m["stats"]["mappings"]["search"]["properties"]["harvest_date"] = d_map
# m["stats"]["mappings"]["fields"]["properties"]["harvest_date"] = fd_map

# for k in m["stats"]["mappings"].keys():
#     m["stats"]["mappings"][k]["date_detection"] = False
#     es.indices.put_mapping(index="stats-2.0.0",doc_type=k,body={ k: m["stats"]["mappings"][k] })

# def alter_fields(d):
#     action, data = expand_action(d)
#     for f,fn in [("date","harvest_date"),("recordset","recordset_id")]:
#         if f in data:
#             data[fn] = data[f]
#             del data[f]
#     return (action, data)

# reindex(es,"stats-1.0.0","stats-2.0.0",chunk_size=10000,bulk_kwargs={
#     "expand_action_callback": alter_fields
# })

# 2.0.0 -> 2.1.0
# source_index = "stats-2.0.0"
# target_index = "stats-2.1.0"
# m = es.indices.get_mapping(index=source_index,doc_type="api,digest,search,fields")

# for t in ["search","download","seen","viewed"]:
#     m[source_index]["mappings"]["search"]["properties"][t]["properties"]["total"] = {
#         "type": "long"
#     }

# for k in m[source_index]["mappings"].keys():
#     m[source_index]["mappings"][k]["date_detection"] = False
#     es.indices.put_mapping(index=target_index,doc_type=k,body={ k: m[source_index]["mappings"][k] })

# def alter_fields(d):
#     action, data = expand_action(d)
#     return (action, data)

# reindex(es,source_index,target_index,chunk_size=10000,bulk_kwargs={
#     "expand_action_callback": alter_fields
# })

# 2.1.0 -> 2.2.0
# source_index = "stats-2.1.0"
# target_index = "stats-2.2.0"

# def alter_fields(d):
#     action, data = expand_action(d)
#     for f,fn in [("date","harvest_date"),("recordset","recordset_id")]:
#         if f in data:
#             data[fn] = data[f]
#             del data[f]
#     return (action, data)

# reindex(es,source_index,target_index,chunk_size=10000,bulk_kwargs={
#     "expand_action_callback": alter_fields
# })

# Migration to test
# source_index = "stats-2.2.0"
# target_index = "stats-2.2.0"

# m = es.indices.get_mapping(index="stats",doc_type="api,digest,search,fields")
# for k in m[source_index]["mappings"].keys():
#     esb.indices.put_mapping(index=target_index,doc_type=k,body={ k: m[source_index]["mappings"][k] })

# reindex(es,source_index,target_index,target_client=esb,chunk_size=10000)

# 2.2.0 - 2.3.0
# source_index = "stats-2.2.0"
# target_index = "stats-2.3.0"

# m = es.indices.get_mapping(index="stats",doc_type="api,digest,search,fields")

# for k in m[source_index]["mappings"]["search"]["properties"]:
#     if "properties" in m[source_index]["mappings"]["search"]["properties"][k]:
#         if "total" in m[source_index]["mappings"]["search"]["properties"][k]["properties"]:
#             m[source_index]["mappings"]["search"]["properties"][k]["properties"]["total"] = {"type": "double"}

# for k in m[source_index]["mappings"].keys():
#     esb.indices.put_mapping(index=target_index,doc_type=k,body={ k: m[source_index]["mappings"][k] })

# reindex(es,source_index,target_index,target_client=esb,chunk_size=10000)

# 2.2.0 - 2.4.0
# source_index = "stats-2.2.0"
# target_index = "stats-2.4.0"

# m = es.indices.get_mapping(index=source_index,doc_type="api,digest,fields")

# for k in m[source_index]["mappings"].keys():
#     esb.indices.put_mapping(index=target_index,doc_type=k,body={ k: m[source_index]["mappings"][k] })

# def alter_fields(d):
#     action, data = expand_action(d)
#     if "index" in action and action["index"]["_type"] == "search":
#         try:
#             new_data = {
#                 "records": {
#                     "download": {},
#                     "mapping": {},
#                     "search": {},
#                     "seen": {},
#                     "view": {}
#                 },
#                 "mediarecords": {
#                     "download": {},
#                     "mapping": {},
#                     "search": {},
#                     "seen": {},
#                     "view": {}
#                 }
#             }

#             new_data["recordset_id"] = data["recordset_id"]
#             new_data["harvest_date"] = data["harvest_date"]

#             for k in ["search","download","seen"]:
#                 if k in data:
#                     if "total" in data[k]:
#                         new_data["records"][k]["total"] = data[k]["total"]

#             if "viewed" in data:
#                 for t in ["records","mediarecords"]:
#                     if t in data["viewed"]:
#                         new_data[t]["view"]["total"] = data["viewed"][t]["total"]
#                         # this is on purpose, for view, count and total are equal
#                         new_data[t]["view"]["count"] = data["viewed"][t]["total"]
#                         new_data[t]["view"]["items"] = data["viewed"][t]["items"]

#             data = new_data
#         except:
#             print action, data
#             traceback.print_exc()
#             return None
#     return (action, data)

# reindex(es,source_index,target_index,chunk_size=10000,bulk_kwargs={
#     "expand_action_callback": alter_fields
# })

# 2.4.0 -> 2.5.0
# source_index = "stats-2.4.0"
# target_index = "stats-2.5.0"

# m = es.indices.get_mapping(index=source_index,doc_type="api,digest,fields,search")

# m[source_index]["mappings"]["api"]["properties"]["recordset_id"] ={
#   "type": "string", 
#   "analyzer": "keyword"
# }

# m[source_index]["mappings"]["digest"]["properties"]["recordset_id"] ={
#   "type": "string", 
#   "analyzer": "keyword"
# }

# for k in m[source_index]["mappings"].keys():
#     es.indices.put_mapping(index=target_index,doc_type=k,body={ k: m[source_index]["mappings"][k] })

# reindex(es,source_index,target_index, chunk_size=10000,bulk_kwargs={
# })

# print json.dumps(m,indent=2)

# 2.5 Move
# source_index = "stats-2.5.0"
# target_index = "stats-2.5.0"

# 2.6 Rewrite

source_index = "stats-2.5.0"
target_index = "stats-2.6.0"

# m = es.indices.get_mapping(index=source_index,doc_type="api,digest,search")

# re_map = {
#     "http://rs.gbif.org/terms/1.0/Reference": "gbif:Reference",
#     "http://rs.tdwg.org/dwc/terms/MeasurementOrFact": "dwc:MeasurementOrFact",
#     "http://ns.adobe.com/exif/1.0/PixelXDimension": "exif:PixelYDimension",
#     "http://ns.adobe.com/exif/1.0/PixelYDimension": "exif:PixelXDimension",
#     "\"http://purl.org/dc/terms/identifier": "dcterms:identifier"
# }

# dk = m[source_index]["mappings"]["digest"]["properties"]["keys"]["properties"]
# for t in dk:
#     for k in list(dk[t]["properties"].keys()):
#         if k in re_map:
#             dk[t]["properties"][re_map[k]] = dk[t]["properties"][k]
#             del dk[t]["properties"][k]

# m[source_index]["mappings"]["api"]["properties"]["recordset_id"] ={
#   "type": "string", 
#   "analyzer": "keyword"
# }

# m[source_index]["mappings"]["digest"]["properties"]["recordset_id"] ={
#   "type": "string", 
#   "analyzer": "keyword"
# }

# print "Copying Mappings"
# for k in m[source_index]["mappings"].keys():
#     es.indices.put_mapping(index=target_index,doc_type=k,body={ k: m[source_index]["mappings"][k] })

# def alter_fields(d):
#     action, data = expand_action(d)
#     if "index" in action and action["index"]["_type"] == "digest":
#         if "keys" in data:
#             for t in data["keys"]:
#                 for k in data["keys"][t]:
#                     if k in re_map:
#                         if re_map[k] not in data["keys"][t]:
#                             data["keys"][t][re_map[k]] = data["keys"][t][k]
#                         else:
#                             data["keys"][t][re_map[k]] += data["keys"][t][k]
#                         del data["keys"][t][k]    
#     return (action, data)

recordsets = [
    "00038086-3303-4b89-b2d6-1b84e7598f0e",
    "0072bf11-a354-4998-8730-c0cb4cfc9517",
    "00d9fcc1-c8e2-4ef6-be64-9994ca6a32c3",
    "00df4fe2-0025-45ec-814a-36777155e077",
    "00f55b32-0b1f-4878-b767-56c0680dccc2",
    "01017cef-5065-48b2-9db8-3e428971d702",
    "011880b9-7697-4626-946f-258a68f754cb",
    "011a5f7c-663d-4fed-b11e-58b3b6610005",
    "013c6135-cb98-45f5-ad86-98e4050481a8",
    "01545022-096d-40bf-bfe6-ade65f608b26",
    "0191c073-6b37-4d9f-b610-92301f34d9ce",
    "01dfe0f4-24fe-447e-9f8f-1db7f8394b89",
    "0213fb12-f650-405c-b3eb-e34db3989961",
    "021e2617-7532-4cef-806c-690bed32ab84",
    "0220907a-0463-4ae0-8a0b-77f5e80fff40",
    "024164ec-7d39-446b-98f3-8d46b85568f5",
    "025a810b-28f6-427d-b342-16fdf5f74f4b",
    "026a4216-957a-4efb-acf1-506499ec474e",
    "0272afc1-36ee-4899-8c28-dde9d8a211d9",
    "029e1b92-bd6c-4037-9a0b-10136a879a74",
    "02cdbad4-154e-4b8b-b0f7-aac8dfd515e8",
    "02f4e028-eb2f-4c4f-9b08-339c40ebbe10",
    "02fceae6-c71c-4db9-8b2f-e235ced6624a",
    "02ff95c9-5a14-459f-a2e4-7f93d87a7559",
    "0377a9ab-eea0-4580-8c6a-a33c88647122",
    "0395f3c4-0277-43b6-a36d-e07720588790",
    "03eac319-23c7-429e-9fa4-480640007d62",
    "03f8cc9d-ef51-42ca-82c7-5d78750e583e",
    "04427b66-9027-4bc3-a019-e6a20212a0cb",
    "045aa661-f985-4203-80ff-98daafdfe377",
    "046a2685-be26-4d6e-80cc-d95e907922fa",
    "04d9b721-259c-4d6b-b48f-2e23edf66c9f",
    "0506069d-2517-4666-b315-b1645ba130e5",
    "05498053-5a06-45d5-bf6c-dbea1c42cb2b",
    "0568a2f7-d5df-4b03-89e9-9c162c17a668",
    "05837c30-ef5c-4972-bb2a-b44182e4e70b",
    "05c029de-734c-450a-a41a-56061b7ebb18",
    "05d06022-2d60-44f1-a428-e32c0c11c589",
    "061dfaff-0c45-459e-ac56-738fe4cbe213",
    "063825dc-b8c3-4962-aea4-9994bcc09bc8",
    "0669bca4-e15e-4775-a0ee-1a342912b091",
    "06bd4115-8879-430b-afa4-8ac8ead5600d",
    "06c35934-1b75-4196-838d-29d509951bf9",
    "07049fd3-83e0-4d24-bbe2-40842178f293",
    "076c58a9-5bd0-473b-87ab-a5cafbbc10a4",
    "079e516a-5396-4dc0-a3f8-fff46900d981",
    "07c47dd3-a0a9-434a-b144-71c32996f278",
    "080aa588-2f4c-4d65-8a55-f0b83e8aa7e6",
    "08bfaeb8-abb4-4b33-b0e7-ed1242377bbd",
    "08fc0e5e-941e-4ad0-8ce1-bcee8fd4f4ca",
    "0901af29-834b-4502-91c8-f3b0f56be2bd",
    "097df9e8-f6d1-42d4-ae23-bd62756bc567",
    "09a3fcf2-55a1-488f-aa42-f103bdce0536",
    "09adc842-a7e1-4e2e-9c26-e56ca7349ccf",
    "09b18522-5643-478f-86e9-d2e34440d43e",
    "09edf7d2-e68e-4a42-93da-762f86bb814f",
    "0a0e4054-0568-4352-9ef1-baa72d577ee5",
    "0a0f5c81-bf4d-492b-b459-08bd987a0c9a",
    "0a410c4a-cd4f-4bd8-b6ba-a0c2baa37622",
    "0a45f2f2-2a6a-48ad-a393-1d72d1b740ba",
    "0a52d848-e70c-4694-9f4f-f395fcdae0d4",
    "0a854fba-3da1-4d7b-88e1-1204a993ee00",
    "0a8f691f-004d-41d8-89fb-9f808a8268c5",
    "0b065abc-f9a3-400f-a36a-e3bfc4effc82",
    "0b17c21a-f7e2-4967-bdf8-60cf9b06c721",
    "0b263d0e-6571-4883-946e-78baf248eb63",
    "0ba2c5fa-e6da-4256-adb1-4b78e46b5531",
    "0bada388-4adf-4b8c-b733-0a1bfc7c233c",
    "0bc60df1-a162-4173-9a73-c51e09031843",
    "0bffd75c-2c42-4119-bae7-1ca6d8eb4d1a",
    "0c15e83e-79ee-4ee3-86e3-e5f98a51dc11",
    "0c9378fa-3ccb-4342-be2e-5b5080691dd1",
    "0c94911f-6f18-40a2-a0c3-95c845bc41d7",
    "0cf71170-c4ea-4fb2-b13e-66a4f62d3a2d",
    "0d05a365-36e8-4150-a350-23ed33f79b17",
    "0d370ee9-1006-4a74-b504-77d3c0b6828e",
    "0d674006-396e-4017-b481-1acd9d7f6db6",
    "0dabd609-2505-4492-8b7a-3f8301d8d5e1",
    "0dd7c8dd-412c-4a13-a1d3-47e1e1af5455",
    "0de2c826-1053-491e-8715-36653017217f",
    "0e0e9bbc-1dea-4de4-95ae-aecc90844bbf",
    "0e13a337-c0f4-4420-af26-983ff44f0966",
    "0e2f3962-e905-48f2-a1c6-19d16e2bd5ba",
    "0eba218d-bccb-45cb-95e2-cc5625be106e",
    "0ec58818-0c78-4804-a899-632f103371d8",
    "0ed0b942-198e-4500-8fe0-1d1ef0785454",
    "0ed8a17e-149b-4cbe-8383-7676da92ea1c",
    "0f19f6d6-79a4-434e-ba0b-a4f49f334078",
    "0f3f26e2-cc13-47a3-a268-4c321b621586",
    "0f53b3e3-c248-4026-a070-15c3fefdbbc0",
    "0fb53db0-a7a4-4ac4-8ad3-bc4648b411e0",
    "0fcbf959-b714-4ba2-8152-0c1440e31323",
    "0fd6e726-6828-4f62-ba8d-6ec316fe0b52",
    "108cd12c-30c3-4b18-8af8-0971221f3946",
    "10a02814-9469-42fd-b074-22ca6c8271c7",
    "10eea9b5-e08f-4c07-adf6-561aea89ae09",
    "115ba8cf-1ab6-4142-9f11-a53d629c656d",
    "11d3ad3b-38de-4709-8544-ec3c26d96607",
    "1200dfa1-c9d1-42c9-9be0-2b399f0297e3",
    "12bcfc95-b897-42b1-870b-7945e3438502",
    "130b00ba-757f-4a49-b46a-5434a6a40f2e",
    "1346cd0c-4893-4974-ac25-0320697ed0e2",
    "13517230-f4e1-459c-b944-00b39a57a081",
    "137ed4cd-5172-45a5-acdb-8e1de9a64e32",
    "139f2c47-4051-4c44-b95a-45fd20b1a8b9",
    "13bac7f8-ff25-4d02-82d7-516dc6355beb",
    "14312086-24f8-453a-be6f-d7a0c796a116",
    "147f3f52-4399-474b-b43d-8379c680d67b",
    "14a8f79f-eab7-48da-ad50-bda142703820",
    "14f8f83f-7a0c-458c-b6d5-6da7dc8eaa0a",
    "15029ee7-cc08-4719-ac30-893d2c5d47b4",
    "1527b668-b797-42be-94d3-0058e1393e94",
    "155593e5-e984-4b4e-9c13-5b7d2849320e",
    "155720bd-a5c4-4ae8-ad79-4d5c0a4433c2",
    "156e4092-a68c-4b6d-a3a8-a36edcd74867",
    "15a1cc29-b66c-4633-ad9c-c2c094b19902",
    "15aa4812-aad2-4b26-a1d8-d4f8d79e6163",
    "15b5a0a1-3883-4789-a351-294f00282137",
    "15bce43c-7ada-41fd-b85f-223dc24d54f6",
    "15bfc444-a60f-4eda-83e1-747ab1308107",
    "15e04168-22cc-4283-9042-247ab053c7ca",
    "161b43da-34c8-4952-b363-be9d09e46cd4",
    "161ba5a0-43f0-41ae-9cd5-3fc42c9a6d5a",
    "16b3f74c-0396-4420-98bd-7ea8d256430d",
    "1701a75c-5a57-48c3-84c2-234a53f4c3e2",
    "176d536a-2691-47c4-95c1-c0d47d3abd48",
    "17969b7f-c1d0-4c84-9cbc-de64b90a62a5",
    "17cea35c-721f-4d9b-b67f-d29250064d25",
    "17d26007-b163-4d5f-ba19-47bc47f66616",
    "17fc477d-727e-4dde-99d6-ac440e937d14",
    "17ff84d1-e3e9-43d1-a746-745ef8d339d0",
    "181352ea-3598-4f32-b919-c8f6097f4c65",
    "18439dec-e9dc-4732-b1c1-0c7007754dbe",
    "18c215c3-c02f-47e9-bb66-196c33c8f672",
    "1908b316-4c07-40e3-a3f4-f8dcf2d42ddf",
    "191206d2-a389-471c-9f9d-c0b8b00c35f8",
    "1959a748-5ea4-48fd-902e-3ebf57b66ddd",
    "196bb137-2f82-40d5-b294-e730af29749f",
    "196c4f1c-53f9-480f-a012-dc0522629047",
    "1977deb3-c3ac-4b2c-99a3-6337b3c11907",
    "197f7bb8-213d-4fae-b536-652dd91e56dc",
    "19daf0a7-5e37-41e6-97f8-4494553c358c",
    "1a8eea37-7c72-4032-a38a-254154449ad1",
    "1ab49097-5cce-4524-a0c2-2cb18beda9e5",
    "1ad40bde-8a2a-46bb-9252-0cdc53df5683",
    "1aee98c2-6623-49ae-b4af-c8afcf08150f",
    "1b6bb28e-e443-4cd3-910a-c6c43849c2cd",
    "1b8d8ab6-cee0-4986-a820-f7960323f6f9",
    "1ba0bbad-28a7-4c50-8992-a028f79d1dc5",
    "1bb2a80e-271f-46a8-bbfc-ba56d3e6292a",
    "1bb33d2d-0714-4fc9-968e-b66bab1cf3d3",
    "1bc74afb-698f-43a7-90e6-352dba6c74da",
    "1c813b16-2c8b-466c-897f-883288cfef41",
    "1c8d18f4-5af2-4d86-98d2-8a5ed06456e2",
    "1c8ec291-8067-4b48-848b-410c2c768420",
    "1cbd3c1a-df01-494d-ac85-74a9b4a7820c",
    "1cd5279b-37ba-4177-a02c-6d1d6e2c6bcf",
    "1d14acd1-20ef-4a55-8206-f04c8a75ea3e",
    "1d17fdad-d338-4ae0-9232-dbf18eaf9f66",
    "1da2a87d-4fc7-4233-b127-59cb8d1ca5ee",
    "1db8b527-7713-405b-b7ec-bc824580ccc6",
    "1dfee34e-ea6a-468a-a4ff-ab525aa6b9e8",
    "1e054b9b-0193-4ff3-b623-9264cf982d4d",
    "1e169b34-cda0-4b42-b9cb-8e6fc61e8835",
    "1e69489f-c371-426e-b0ee-b9eb7ad7c4a6",
    "1e6c8187-1521-4501-b205-ac8f513d5e04",
    "1e86442f-35a5-4e7b-9a38-4599e4d3b510",
    "1ea148f3-17d9-4af8-b49f-bb17affe24d8",
    "1eb85009-7d5d-4663-8c74-d87b62290365",
    "1ebb0c8e-31f2-4564-b75d-65196bee4f09",
    "1eca069b-09e0-406d-9625-cb9c52e1e5cc",
    "1f2b44b8-8556-4d6e-8247-4611689551cf",
    "1f3cbd9f-ba74-443d-b8ca-48a1ba5eb159",
    "1f9209f0-ecf2-4c3e-8ec5-bfacec1e5b9c",
    "1ff3b226-6d1a-4f5f-a81c-490da4ba4aeb",
    "1ffce054-8e3e-4209-9ff4-c26fa6c24c2f",
    "20360fae-574a-4d63-b9f6-47b1cc07fd22",
    "204fbebc-37cc-4331-a2be-11f38949561c",
    "205fa34c-2fcb-4492-b992-972b18560f6f",
    "207b6c64-7b58-4d6a-816d-bc759c27eafc",
    "20891266-b337-48e4-8121-0dbf6f83f155",
    "20b209dd-08fe-41a5-98bd-9b53355e8eb6",
    "212dcc45-bf5b-43d8-a804-3351c04c2f7a",
    "215eeaf0-0a88-409e-a75d-aec98b7c41eb",
    "2195056f-529f-4035-a537-f11aae18d6b9",
    "21b4db0c-ab35-4b2d-ab10-e89a797b42f6",
    "21ea1ef6-4a2a-4ff2-a18b-5c0f297fc1cf",
    "22e6b924-9bd5-4388-9404-8d83a3f03ecd",
    "23063b94-3fcd-4509-bca5-3c40a0e06758",
    "232c938d-4ca0-4048-9628-6fe461ad614a",
    "234cd0a6-7df2-4375-87fe-04c0d6944a4d",
    "23670729-9bb0-40cb-aa89-ef2ae95535ee",
    "237b6f32-98af-4835-9301-446100d9cb9d",
    "237bd113-32f3-4091-9710-4a1b074fe26d",
    "23b85d9d-4669-40db-901f-aaad686fe0b8",
    "23ca5883-852a-40ab-b8d3-7e4985202844",
    "2417de25-d725-48fc-b969-843ad28a4450",
    "241d64f1-480a-48ae-8ec2-cd12af4a16e9",
    "24c1a8f8-00bd-4121-897d-1fbb2db76ba2",
    "252a0a12-f114-4fb5-aa9a-678c523d6dcd",
    "252e1e9e-4bb0-4baa-bdaf-fa39b7900c69",
    "253f90be-3b94-469c-820c-cb727b85bdd4",
    "25cd5e12-7830-4f46-bf6d-9b6deb706f44",
    "26279ecb-a713-4d30-a484-54d967a3b52f",
    "2658bee1-521d-4e99-acce-37c08d54bc3f",
    "26ab2787-ba36-45df-b25d-29e93bc29b4d",
    "26f7cbde-fbcb-4500-80a9-a99daa0ead9d",
    "27065e11-3d67-4f34-b9e0-2e6c7314856d",
    "271a9ce9-c6d3-4b63-a722-cb0adc48863f",
    "276b0c62-dfde-48e7-8f4c-18f277dbf7f3",
    "27b2b46f-4cae-4ffa-870d-b17e51d627f2",
    "2823b0c8-dd5f-487b-a0d0-7411005a4eaa",
    "285a4be0-5cfe-4d4f-9c8b-b0f0f3571079",
    "286cbc36-5ca6-46fe-b56d-13b43a229b25",
    "28748d79-e525-4023-99a3-8b017725afdc",
    "28a8561d-4699-4c90-823b-686d6207d675",
    "28ddc5cc-5aa4-42cc-a29d-4d516de9ba86",
    "29388da4-4d07-4aff-9e90-560556b0b0db",
    "295a8445-346f-4ea2-b0cf-4a3863c72cdb",
    "29b8da72-4420-4f65-b755-a006a23cf65a",
    "29d217e3-754b-4a72-9e57-5cd05312e7c0",
    "2a848917-2a01-4c67-82ae-95512538f868",
    "2ab518a2-d1fc-45ac-8225-d72ce50a497b",
    "2af72a37-04f1-4208-a50b-c05e84f47f1f",
    "2b03a9d6-3575-43ea-8f43-38cbe0ee72e6",
    "2b21081c-d5e9-49bd-b29a-0b6e4a551b78",
    "2b28e5af-222d-4cf7-b98d-d18fa73d31d8",
    "2b89de41-42bd-46c6-ab61-d386f855f7fb",
    "2b946e47-7b37-48dd-83bb-fd892c026f9f",
    "2ba96485-62cd-453c-a317-e7c220b7202e",
    "2bbafa00-3162-4e8e-947c-64a13b8d3fef",
    "2bf0d628-1125-4c32-b634-87985cfd9459",
    "2bf27fdd-cc7f-4d00-a9af-393ce94f6c3a",
    "2bfc480c-e5b3-4a9b-9587-a92c22830ace",
    "2c00c297-9ebd-498a-b701-d3ebde4b49f3",
    "2c00d087-1df6-4744-807d-056be36eed0d",
    "2c07346d-2239-479b-91e4-787a8f589457",
    "2c2cc29c-3572-4568-a129-c8cbec34ccbe",
    "2c662e9e-cdc6-4bbf-93a5-1566ceca1af3",
    "2c9aee69-4898-4b7e-8f1d-90bf0ac09ecd",
    "2ccbe8d3-c688-4c20-bf24-68a5ef486519",
    "2cebadf7-6d52-49b2-b3a7-d4969a36aa12",
    "2d2abb35-06b6-4fc4-beb6-edf3b841acf2",
    "2d2f8a69-b58f-4320-9cdf-8a7b87219fa4",
    "2d4658e3-0d1a-43fc-97ff-b4813dd1f86e",
    "2d47501f-dbed-43ce-a9c3-9c8542648ce4",
    "2d5588f4-bf5d-4fba-adf2-d825af34fd38",
    "2d853a6d-50ec-4931-8e91-48fc2491fdee",
    "2d86bdb0-a563-4a35-b990-469e9e896712",
    "2d94a3ac-f505-49ec-98e7-3b7dc48344dd",
    "2ddc799d-4c83-4438-8c2f-8f4a815dd98e",
    "2de463f9-9028-400f-a813-a3c0e468b62d",
    "2df867b1-89de-4539-8414-67c47a88f0c8",
    "2dfd653c-d582-46ab-9835-0ae2e2a03e2c",
    "2e185eda-1790-45e3-88d6-261304c37ed4",
    "2e3058eb-adbb-4952-8c67-edfc365b9a45",
    "2e3a8e5c-eef9-462d-a690-3a91fe111e13",
    "2e4ccf50-bb7d-43a6-9640-088b248c2c5a",
    "2e65e24b-b7e2-40a4-a40c-09edafc1e3f4",
    "2e6b6643-ebc7-4a80-a7ea-f4dd7b9c42e7",
    "2e746628-f895-4367-ae31-62e81e0b6b98",
    "2eb8ff2f-4826-4fc3-be68-22d805bcae88",
    "2eba7b78-977b-4bdf-bef4-78dbe2c6265f",
    "2ec3b31e-c86b-4ce9-b265-77c8c3f9643c",
    "2ecec1f2-9598-4c39-9f12-8f9158c93d11",
    "2ee56f8c-db1d-43aa-bfb7-3fb2a19601df",
    "2ee6534e-46ab-4233-98c4-d13c4262ce2e",
    "2f484dca-4e55-4d29-ad96-56c96573444f",
    "2fe72860-9220-4acd-894b-81b4d98a5e24",
    "3027c437-cdb3-4072-9410-5a46ec3b1fd5",
    "3056112e-97c6-4d0d-b6c2-3c0a9adaca24",
    "30733214-8eb0-4894-b19d-775fe8a617cf",
    "30793cd1-ee76-4fc3-b64e-30880a3ca4e9",
    "3096b21c-b2e8-46a3-9b20-e8d8b64d9d85",
    "30ab9c2a-0b54-4c04-84ca-bc7abdd90b52",
    "30ae66f4-b3eb-44ce-87aa-309d47e5facb",
    "311c3a01-c824-4a85-8771-fcc3f353619b",
    "312cf3f1-2913-4c63-9ba8-0b870cd3c120",
    "313a83e6-a200-41f2-9945-2124000111f8",
    "314f66a9-2b8f-4085-b10c-0f083ce2f1eb",
    "317a7992-b597-4546-aa69-0aee0090fa76",
    "319636db-c2da-493c-beac-1194949e95b4",
    "31c140bc-e6f1-4acc-beaf-b825cf288ad9",
    "3224d5b1-02fe-42ef-b360-5df20007420d",
    "32c7f9e2-a045-4737-aa7b-49d33a25e8f2",
    "32ca79d3-6335-4b03-a4dc-da3710e293bb",
    "32d433aa-9e2b-4ff9-bc55-5c3e30112207",
    "330e9fef-e327-41c4-8981-074205ea5eac",
    "331b6d1b-842e-4c63-aa23-75ef275d8a9f",
    "332cc28d-5c5b-44d9-88e4-038f49559ec4",
    "333ac26a-30bc-4e0c-a6ef-c57a40f6bd99",
    "33d00b3b-e0b0-4e22-9a22-83583612240c",
    "33fd0737-6207-42cc-bc64-cc637266b476",
    "341611f4-8b65-4655-b244-9be91a1109cd",
    "341da8fa-d049-46ee-9be8-463043f26fa7",
    "3420d3d5-f142-4db6-951c-5d37cb72ce53",
    "34344d35-1857-4ef3-924e-bfab3c2524fd",
    "3451a762-d117-430e-968c-dd747ed53887",
    "347579f4-d44a-4c8e-a578-09c2a8132573",
    "348f4784-4786-45be-8d0f-85f2b189eba8",
    "34cad268-8226-4280-b637-dde38c82a29e",
    "3501e0a6-1420-45b9-bbf9-77349e79e9d7",
    "35159359-7113-4e04-b9f5-361cf24896e3",
    "3530e017-981e-400d-b9da-6f5af5dce626",
    "35830c1e-429e-4006-a153-78984a3e0ee2",
    "3583a122-27b5-4028-91d2-fe02a706c4fa",
    "35879d2c-063f-4046-9ac6-eda6410e21a9",
    "35b90b0e-7908-4d2b-800a-2bf6603848a1",
    "35c43eda-1f4a-4713-bb69-e3fbe1bf792f",
    "35ff9d4c-829f-42c9-b8ab-8dbe1a69e0d7",
    "3617a6a3-d384-48de-948d-2d1e1c54e090",
    "361943a1-c123-4057-b065-7de641428c99",
    "361efdf7-9845-411c-90bd-e51ec7991e87",
    "364a24d9-d4a8-4e0b-8e50-07b90f844548",
    "364b1f8d-5975-48d9-bba1-c97ab172986c",
    "36aff13c-6653-4fd2-9605-ce0dabbadc4e",
    "36be338b-cfb2-47e4-a1fc-b3f7a1aaaf22",
    "36d35b23-113e-4633-90ec-19d265a3b5f6",
    "37311716-059e-4c4e-af61-a3fb90b243c9",
    "37963235-6252-4688-bb53-863de38918e7",
    "37d4d085-d8be-4826-9bc4-c6a36557fa70",
    "3872f27e-cf4e-40bd-b91b-ba7b723a86e5",
    "387ae47b-8641-4e3c-ae66-d1e20d93cf32",
    "38b67ced-5220-44e9-bb4b-83736e7316a1",
    "38db50bb-72f3-4416-aeb9-61457e655a6d",
    "39023cd0-ca46-4235-a6fa-162e414d6483",
    "390e1849-d2e6-4151-9e2b-dfa8ff2acd5c",
    "39289378-eed8-442c-ba0b-fce8b1679d8f",
    "39de54ef-ecfe-4e9e-9f9c-d4b29f0a893f",
    "3a0018d0-9b91-4671-82d2-b635177dace6",
    "3a9fb382-e7e4-4bb2-8721-b7323252ddd9",
    "3ad82604-c4b3-4fd0-b03e-d8f874062146",
    "3b057b81-9c69-4aed-8a73-b9621834f3c4",
    "3b946cd4-28f4-4194-a313-100b388fcad8",
    "3b9ecf1e-3c04-4d8b-84cd-9ae48e70e13a",
    "3bba1a50-41af-4569-a2da-709652f2e8ca",
    "3c367a2d-eec0-4ef1-b3bc-4cbebb320c5a",
    "3c6099e4-3a0c-44c8-942a-f44860aea12b",
    "3c6f1ea5-f2e7-4203-9cfe-74ec2fb1b035",
    "3c919328-94fd-4657-b81d-21f4707253ed",
    "3ce2cebc-1b50-4c64-84a2-8aa2f0530ff6",
    "3d001358-9707-437b-bca9-eae32ac8556c",
    "3d2b5be0-7c1d-4693-9226-94bc0061123b",
    "3d4401cb-b3dd-48bd-912a-709bb4ecf5f8",
    "3d9a5640-4285-46a6-a371-5ebc8dccee8f",
    "3dd86ccc-556c-4e35-8c52-b2b85005f16a",
    "3e5a9f79-297b-497d-84eb-97e0d1e5c2bf",
    "3e6559e6-6929-495f-9f32-80d021d11ae0",
    "3e86e072-2597-4849-87d5-565afe40f988",
    "3f1100f3-c4be-4c94-af61-bd0d2d011b8a",
    "3f2e2cb9-3b52-4114-b941-54f23e49ca09",
    "3f321a4a-de68-465c-ade5-763b99b3db3f",
    "3f419231-1ad8-4432-bd27-a95eb0fe0dd8",
    "3f508496-c860-4701-93e4-84e940c8395e",
    "3f67e6b3-1ffa-456e-944c-0f2ed6373302",
    "3fe3a250-0f48-4a9b-bb71-36d798694912",
    "3feab0ae-cfc3-40fb-b681-018c410f1996",
    "3fedbb40-3988-4665-bf86-6b1c89c57215",
    "3ff3bf5c-7aba-40c3-80b2-1b00ea1abdd5",
    "400474eb-e6e4-4f95-9e44-7f4c1efa0965",
    "401fec56-515d-4fa8-87d1-507e742f4f6f",
    "40250f4d-7aa6-4fcc-ac38-2868fa4846bd",
    "40522658-7f8c-4a40-91ed-6754d8084ede",
    "40987883-03cf-494a-a5cf-7c77c7aadb79",
    "41350373-fc6f-4dd9-b908-27805fff9155",
    "414a1846-2732-4c66-89f9-df15fecb4c18",
    "414a5bf5-061e-4e47-8410-0f76a04f7d1d",
    "415c8d47-3695-4421-bf7f-5eba5816b1ac",
    "41800344-33b3-4201-b9d2-cabbbf564fbc",
    "41b119de-f745-482d-be42-a0155bc76e5d",
    "41b166d5-ce08-4efe-99fc-6df77d8fe29e",
    "41e1a09b-bd55-4d20-a480-5d8187f7afca",
    "42deefe0-efb7-4c17-ae0a-3c65f080ea82",
    "42e3ae96-c8b9-44f6-9212-f6a111426d54",
    "42f42c53-e806-4a2a-b4cd-1d965b08651a",
    "4305303f-976d-4074-accc-91e205435cc8",
    "431b56fc-d016-459a-97ab-1e9c8168a7f0",
    "433646ab-571a-44f5-820e-25e0736b1113",
    "433d3c37-8dde-42e4-a344-2cb6605c5da2",
    "437826f3-69f9-43d9-b3c3-c0de0e26cd88",
    "43aa1339-67e4-4298-b7c5-3d0f201266ef",
    "4401544a-ba74-416b-b404-37e94c2fd2bd",
    "441148e8-3713-4966-9f6e-71b855b29acd",
    "44328ef7-fc7f-4ff6-b51b-ed9049857e11",
    "4438ad00-6ba8-4900-8793-3b5c182150b4",
    "448d190c-ff4d-4b81-9a21-df1e548df8a9",
    "44a1d06c-7a0d-4673-9759-d21a486a0772",
    "44eafd0b-da69-4e3b-8c10-4a63397ae40c",
    "4520451d-bd3b-4095-b777-24a420d442c1",
    "4523e216-ee13-4b15-a3f7-a6fd56431604",
    "45544aa4-8762-4bf0-bfc6-890d08dc6ead",
    "46374ee7-7c70-48d8-bf16-2e6c1626565e",
    "46799102-f7a0-43fe-8406-800a55e5d55b",
    "46be8190-0533-4fc7-9cab-1faa26d9906e",
    "471835cc-feb6-4d05-a8d1-62ce71399326",
    "4744450e-888b-43c8-a697-83538fe38d88",
    "475baee8-ab12-460f-ba4c-abfef4cb216f",
    "479a29c6-e72d-42ca-8934-47b67c1a3586",
    "47ac1531-5213-4848-a32d-5bb396ab9348",
    "47b7bedd-66e2-403c-b844-4c9adb9e3082",
    "47e638be-2983-4d22-b05d-260dd5881e9c",
    "4830ffb8-669a-4717-bec8-2f2374f52120",
    "48b3d60d-dd63-46bf-9b76-a9d4a7d2fe51",
    "48b4b812-c52e-4f47-9327-e761f6fc2e28",
    "48c1824b-be3e-405a-bfc6-f7df13b91d94",
    "48e1b8c1-91aa-4b87-8ca0-de1f81232eaf",
    "48fadd42-2eab-4a01-845f-f92f4c8dfe9b",
    "4900d7ce-9442-4305-9889-c9cbbb953eaa",
    "49172c47-a5b7-4d05-bbd2-2934e61533bd",
    "491d9c70-4adc-42a0-b67a-6c051308d185",
    "49c3ac44-7575-490f-848b-daa312d49968",
    "49db9725-7bdc-4c38-8548-c32994e811c1",
    "4a1f1862-cb5f-42c5-b3c8-bfc51b1fd140",
    "4a32531f-1580-499f-bd48-5b0ef4cc5722",
    "4a8d3304-17f9-419e-a6d5-ad98bc2dfcb0",
    "4ac45d7e-c6e5-45ea-a0e0-aea6ebe2afcf",
    "4b05f088-74a4-44a5-a161-8b1484efc240",
    "4b446231-3dc0-4597-a275-b27064d25798",
    "4b92de1f-866d-4b82-af69-37d46753f289",
    "4bec11d1-f8c3-43a7-9e70-ee0256fcedaf",
    "4bf197cb-6fbc-4b22-82d0-849fffb5906e",
    "4c51ed21-26d8-4b3c-bc5d-e49bbee4fa6d",
    "4c9d08ce-71c1-47b8-a572-2d40e5984c49",
    "4cd8e87c-93b6-41cc-9189-50585cdb0518",
    "4cdf5c2f-1a44-4fd5-bdd8-de08c8a660e2",
    "4ce7498b-44c8-44d1-a07d-3d1e5337fc8f",
    "4d89070b-5dea-4a12-8a09-3f65ba33dba1",
    "4db72a36-c08b-4a6b-8c68-ab45ebb0efce",
    "4dce41dc-2af6-448c-99e1-abfd3a9cc3e5",
    "4dfb5828-3653-4604-ac00-db1e1da98b02",
    "4e3043a6-d48a-4a35-b5fb-f67d50cbc158",
    "4e7f6139-8db7-4f79-b8a6-1a72ea7bcea5",
    "4e8502bf-d07a-4603-906a-726fa2240277",
    "4ef6540c-e6cf-4678-bc43-b57435354de0",
    "4efa66cf-8adb-414b-b78c-8e651d20f84d",
    "4f418fcd-01d8-4162-8130-fa911e795949",
    "4f436daa-01d5-4be6-b5c3-fdd255677536",
    "4f8c3594-d7b2-4985-8dd9-1ae77f9187d4",
    "4fcd7093-fe29-4956-b3fc-b53a9277067a",
    "4fecde59-9f59-44eb-ab6f-4a50b4ed85cf",
    "500bc952-76d8-471f-ae26-181775113ab5",
    "50b4c365-5472-4fe8-8825-b1fa0ac57fb3",
    "50cfe20a-9100-4710-89f9-a97bc3aa53d7",
    "511dfafa-94d1-495d-b869-c6ad91536a52",
    "511e8679-993f-4055-9161-ee23d23a579a",
    "515cab85-98d0-4a23-9544-9e7b1c51f5a6",
    "517ca6df-7614-41b0-a6f9-6fbd7f299525",
    "51b958bb-9d5f-48d7-9a97-e372c0c747c3",
    "5235b5b5-5cbe-4de7-8286-bc2e95ba1748",
    "523ead8f-ce76-4b3d-ad66-aadcb16a0a08",
    "524141a7-56fd-4c72-826c-6b0b6fe5e7fe",
    "52598e6d-76ae-4c78-bc5b-12b68a6aa99c",
    "529dcef4-e82e-403e-ba89-a5b9ea22d076",
    "52deaded-c5ee-4db0-afa3-81920e8d3d7f",
    "52fb2f88-bab6-4267-8ab4-ecf452a5a978",
    "53091d18-f173-4fc0-b9d9-20a1494e2466",
    "531537fc-6349-4a20-ae42-540d61797086",
    "534e0618-c8df-4137-840f-74122db111d9",
    "535d4a21-8650-41d0-b92f-b6c028db13e2",
    "5386d272-06c6-4027-b5d5-d588c2afe5e5",
    "538bdd33-b616-4825-8542-7033cc8a185f",
    "53af19e1-9cb3-4834-8974-62adc640491c",
    "540e18dc-09aa-4790-8b47-8d18ae86fabc",
    "54610ffb-b9dd-4c83-aecf-e0864446fb1a",
    "5486b66d-2082-433d-9223-bd789ebca29c",
    "5486dbfe-d4f0-456f-b236-4dfa6d7f29cf",
    "54e35994-c613-4ce7-9afd-0636a88a1857",
    "552ab71c-ee4d-4e42-bbef-5209316a654d",
    "552ce2e5-b627-4d6d-b914-6b495d0a79e6",
    "559e684e-c833-4b3b-a8fe-588fbc9759c4",
    "55d60f69-eee9-4386-952a-805dfb71830a",
    "5626f61a-822e-4692-b432-51f53d053e4d",
    "56879e73-bf9d-4bd9-a7a4-4f2f940d0f62",
    "568e209f-d072-4fd6-8b64-27954b0fd731",
    "570dcca6-a84f-43aa-8053-1a2ac60d9ead",
    "573da104-a5b5-43bd-80e7-69728c0a49fb",
    "57a6bf5f-cda1-41fd-8c12-804c95f74841",
    "57a6d741-de89-40b3-a427-9926e19c215e",
    "57adb0ce-59ae-4d08-ab16-03730bd9fda0",
    "57b1a2a3-78ab-4e69-a77e-a8fd4394ee5a",
    "5835f642-2560-4e3e-9c25-741a12cc3fe8",
    "58402fe3-37c1-4d15-9e07-0ff1c4c9fb11",
    "58619649-7813-443d-9a99-3d4cfac8e0c4",
    "58785b12-2f79-451b-a5c1-8d23f3f65733",
    "5889291d-9105-4740-a30f-2d9d2469c264",
    "589ad4bd-a0aa-4949-bb92-0533ba7edaf2",
    "58afd7df-a696-4dd6-a765-540f8b31c07d",
    "58e4cf5e-ca9c-4496-b4bb-2d52462c3679",
    "59422682-15ba-47e1-99e2-1ef69f7bdd9a",
    "5966b73c-4d16-4cfb-8a8b-437ec5873970",
    "59734d15-8edb-41a4-b3f2-f9bd3407460b",
    "5975bbda-cd92-4084-8a09-ce1e28e6164f",
    "598cda6a-75a8-4ba3-b169-3fa4a285ed86",
    "59a2a8b3-6130-4772-9d1b-e5b895c921dd",
    "5a069d0f-bac7-4d68-bd07-523415611498",
    "5a0d649f-a64f-4e20-a37c-2fe7f9e37bad",
    "5a262e5d-0605-4067-ba71-3fd578c3c6bb",
    "5a660a44-afdd-45ac-8c48-1a6c570ce0b5",
    "5a710161-3760-4f5c-97d6-f42da9f46191",
    "5a9ae910-9e4b-488b-af8e-88074fabc3a4",
    "5aac25d2-bcfb-4084-a700-584311ea539d",
    "5ab348ab-439a-4697-925c-d6abe0c09b92",
    "5ab5f23d-292e-4bea-ba06-12db0f8a8c86",
    "5ace330b-5888-4a46-a5ac-e428535ed4f3",
    "5b015f87-0301-4ddf-b08e-7e92f4969cdb",
    "5b38c6b7-7d52-4337-8eb3-94edcba10736",
    "5baa1d0c-cfc0-4c89-8e3e-7a49359b0caa",
    "5bc623ab-0334-40f9-a4e4-419b7c691b5e",
    "5c861676-8285-4a04-b1c5-94ce73342320",
    "5d307007-02e3-4c84-9e18-6356b50a5a56",
    "5db52f01-8d71-43e5-b835-0ff7d33d715b",
    "5ddcbd44-1802-46b0-bae5-11126409c03d",
    "5e24c385-dab9-40a3-a2dd-2cf8758821b6",
    "5e29dbcc-ce45-4f05-9bb0-212baffa8932",
    "5e2e5b5f-49dc-47cb-b66c-614d8392d04e",
    "5e2f4c81-8c8a-45f3-a220-851f85f86b40",
    "5e73a737-fb7b-4513-8d81-933b0268e1ea",
    "5e7f8e62-b696-4015-aa09-2acc935d748e",
    "5e893602-84ca-4c8c-bac1-99111c777582",
    "5e926e06-23b7-4461-a86c-e9219f5a3606",
    "5ea005e8-626f-47de-afee-972e976cc3a7",
    "5eb57cb9-a6e8-4f84-abde-41a6bfd2080b",
    "5ee6f92f-c65c-4888-98e3-f152b3ceb184",
    "5f105976-5a5f-4a72-850d-2059a80f7c10",
    "5f2b5c3a-1b93-4db4-b6b4-7468f03abee1",
    "5f3bc49a-fdda-492a-a98d-62f084b259ed",
    "5f513dff-ccd8-4578-ad0b-5e6cf035e4d1",
    "5f681b65-9a7e-4b79-8a17-fc95cc26b837",
    "5f6fcfc2-598c-42e8-abb3-50ca9c2446e2",
    "5f916b5e-37be-49ce-bacd-22f1474d440f",
    "5fd109d8-d9f0-4fff-af55-4294d5f6f2f5",
    "605bb19b-7564-4e6d-a5df-8ec841d68ba0",
    "60de7425-67ad-43ea-99b6-1ea0bffa2823",
    "61433fc6-45d2-4912-9c41-10baed78cb08",
    "616857b7-f952-44ef-9b6f-576dc1e65b51",
    "619e420d-acca-40c4-a76b-e8f7d5e84999",
    "61a1c0ce-8327-4e2a-9766-449751a49b7a",
    "61d9b95c-318e-43f1-b924-599872033278",
    "62254613-2696-4834-8c58-5c465f70df56",
    "6226705f-4867-464d-9fab-4e81ecee731f",
    "623a6f8e-794b-44e6-b0f2-1cbf2e58ef5c",
    "6258d160-a7aa-4937-bce3-3538eebd374f",
    "6298ac26-bb8a-4c9c-960c-4966cbf0c597",
    "62a36329-6b82-48bc-94d8-1cb9adb91ab5",
    "62c310ac-e1ff-47bc-860d-0471a84ed0d3",
    "62c35d43-f15c-451d-a8be-1b9c6928b8bd",
    "62f951c1-b6a8-430c-8652-5691f079152c",
    "63263bd7-a975-4bc9-897f-90551c71e5b5",
    "634e3223-6a13-48ff-98d2-95a56f234f32",
    "637d0f2f-a0b4-4f33-a1ad-bd0ab18b620d",
    "63dee426-7b24-4217-a5fc-76428f3aa74f",
    "63e77b2a-9b8b-4958-b9cc-e30364573c37",
    "645bcd10-a74f-4207-a375-0254b954b7ad",
    "645ccfdb-f2de-48f8-a301-19aa41bc680a",
    "64e54f3e-91a3-4a92-ac5b-a03cafbfa83c",
    "64ef2d9d-7ed0-4794-900f-38a73be0ea56",
    "652ea450-af13-4334-96ff-3136d0188778",
    "6539877e-82dc-485c-ad3d-038f383d5431",
    "654e092c-edbd-456c-8cce-2bcbdff71a04",
    "65537538-7a8f-4986-8d57-33054cb738d3",
    "65ab55e3-625c-40ef-8340-1b145ddc07f6",
    "65c412fe-1f9f-4f10-aa75-9845b66ef235",
    "65d603ef-19be-4d6f-92dc-76c5e4220175",
    "662b1aa5-9c19-4eb9-9766-1da78a117456",
    "66427724-af1d-43f8-bc00-cb744e55ac2c",
    "667c2736-bcd3-4a6a-abf4-db5d2dc815c4",
    "66bff5db-d204-4fd1-8f43-6bfd72228a4d",
    "66e00116-15fa-4149-a94a-eb91b98b622c",
    "66fc2613-3189-42ed-b230-ed772e8ff748",
    "676e56d3-6091-4134-9aa0-b03ceea52d52",
    "67b5d248-4a1e-4861-bc2a-3ac7f379acde",
    "683050da-66af-409c-8d21-5fc1fa7e1f73",
    "68592a0b-409a-4334-a36f-ff3294189098",
    "687efa84-c549-4743-a193-72d198d8e19c",
    "68abd0b8-cff1-4c84-a2d9-bd4ac6df4fa4",
    "69037495-438d-4dba-bf0f-4878073766f1",
    "697ed841-1462-46ed-9679-c0c35779e255",
    "6ac89a16-4604-4a78-9047-dcc57e5c0130",
    "6aca6f67-a2e9-440d-a503-9501db6e6f36",
    "6ae199aa-d5fd-403e-aaa8-ce6a9c625e01",
    "6b546055-ecf5-40dd-a091-67fe6f9531f4",
    "6b565194-9707-42da-8052-9f9cf5f9aa60",
    "6b5e29d3-b462-44d8-ba38-d68af5088067",
    "6b6c13d9-7789-4da0-99d7-6786322e2612",
    "6bb853ab-e8ea-43b1-bd83-47318fc4c345",
    "6bed9a04-4abf-4a03-8760-ec12076cd5fc",
    "6c4ec788-c29d-40da-bfe5-326d21a9374c",
    "6c6f34ed-58a4-4ba2-b9c7-34524f79a349",
    "6c778bba-43b8-472f-a0d5-e0357133cbf5",
    "6c77953a-7be7-4145-9f9a-ecdeac5a4d05",
    "6ca3402b-f015-4953-92b7-b1bdc1c36478",
    "6cab4420-11e4-4b55-85ac-6ecfdda70184",
    "6cc31636-c3e5-4887-bbee-2e56621771c4",
    "6cf8b4ee-807b-481b-8044-b7cbd176fa19",
    "6d4b658a-90b4-4639-8b06-b7f07637f6aa",
    "6d5fa9b4-2b88-45b0-b5a9-30dcd9b55651",
    "6d99bdeb-a96f-47bf-8e28-ce1093347335",
    "6d9d4f55-4cb4-4c8f-acc7-b465eb5f703c",
    "6dbb614d-03b6-4d44-be10-db7e4d9288a4",
    "6dbecd06-b0b2-45f2-8bd8-5f9fd0d99af5",
    "6dd2f68a-490f-4281-bee6-dcdefc7b62d4",
    "6dd4be7f-3e9a-4b23-9b5f-5ec0302a0a2a",
    "6e6bec70-a148-49c8-9f97-e64e2dfae5b7",
    "6e6e2b47-fa3e-4bd9-8f1c-105b741d31df",
    "6e922c92-b37d-4c46-8982-19d945ff8fd1",
    "6ed17163-76e1-48f1-9ccb-19cc462c2639",
    "6f264ad7-3b08-4ac1-b7c0-c560bf224949",
    "6f82f182-39b4-4b3f-9087-91f6afafc04e",
    "6fa4b9e1-a3b7-4fc8-848d-5ca8836df0be",
    "703b5bdc-4581-47e3-b4b6-e6f32d0eec54",
    "70f5686f-c1ef-4b9a-a7ae-2536bc7a7766",
    "7110b8ba-0ead-4666-8279-e30f53e343d0",
    "71b8ffab-444e-43f9-9a9c-5c42b0eaa5eb",
    "71c9667c-c0a0-4248-a2e8-80ab5edad740",
    "72019199-f449-48b4-a5bc-1aa892166d7a",
    "72b6dbee-bc4d-4e35-a32c-8df0422771fb",
    "7311c4ac-7cf6-4160-a55c-4a4c7cd0cf89",
    "7340c0df-8829-4197-9dc7-0328b8e7f5dd",
    "7370af7d-07b5-4fc6-9eca-0204b4d6e33e",
    "7400877a-13b8-423d-9074-5371a8234157",
    "74473bcd-2107-493d-8a78-c66b5d3e5061",
    "7450a9e3-ef95-4f9e-8260-09b498d2c5e6",
    "7474bcf0-cab0-4294-b53b-5f3e3415a441",
    "74ba1d92-d9a5-486e-8e52-bb44d51e1788",
    "74f85f55-a6d8-414f-8c57-128c020481f2",
    "750a80fe-60b9-423b-aca1-dcc7937d2c84",
    "751c28ab-c9f2-4f94-b3c8-37a3a09d6664",
    "7559ca29-1d2f-4a64-993f-41939ee40947",
    "7576d3a3-f686-4f47-80b3-e6ce51f8819a",
    "757d51b1-96e2-47dc-a6af-222076e665e7",
    "75a2141b-bc93-4970-bd84-87bf637b1086",
    "75c7a013-8dab-4f9c-ae6d-3a7cc24b67ce",
    "75e3aff5-b3d0-45c3-835c-5ffde192c63f",
    "76015dea-c909-4e6d-a8e1-3bf35763571e",
    "7644703a-ce24-4f7b-b800-66ddf8812f86",
    "765fbb1f-9ac6-4415-8c73-ff1e2272c985",
    "767c78b4-1c16-4cac-ad04-66333ac5a7f2",
    "767e0c9c-3747-4400-896e-26e2ee149b4a",
    "7698ac84-9ba1-48a3-938d-a5086c009f53",
    "76fd34da-4892-4821-858d-98fe9e28ba8b",
    "7710b487-1e24-4d65-b062-1651841b8af9",
    "77200b4c-1148-4801-a58e-d8c2936960ad",
    "772be539-eb88-45a5-b096-96ce92b2fafd",
    "7757c07f-18fd-45c2-84cc-60bd3742e100",
    "778c8bef-370c-4a17-a4e1-bbfad7fff176",
    "77926dc0-a628-49e1-905c-7a2a9283df77",
    "77b762ba-7cda-4617-97d7-e78df7f6dfab",
    "7809d96b-7edf-4ef7-9f12-59967e9a01a6",
    "780c8bfe-482d-4ea6-8346-3076a991116e",
    "781fd581-7b93-471e-a025-413e4bcd8491",
    "7820adf7-b24b-4a25-83b2-f73d897d9050",
    "78945faa-7a10-49ab-88a8-65c435cdb973",
    "789bc19d-00cd-41ec-ab59-be7139dbbfe7",
    "78bb515d-4508-45d6-94e2-e53638ce2fe4",
    "78d74353-0ead-4d58-904e-85709609823e",
    "78ee1a12-9e8a-4d9c-84de-e2dfce4e1447",
    "7915973f-a3b0-4d2c-86e7-02d40647393c",
    "799c4f6d-e178-4274-a05e-2a49f53e7510",
    "799e9d9e-fcde-4e34-93e9-1096669e04c6",
    "79be41bc-8142-485a-9d57-d6195f9a7c81",
    "79dfdec6-3e24-489c-a7ce-85dcc52bc3f9",
    "79e7ae3a-0ab3-4e94-adbd-62e0d4fe2d0a",
    "7a8d946d-083f-4d2a-9cc9-cd590398194f",
    "7ab16fd8-1f09-4c60-a7d6-b71d8c342513",
    "7ad07cff-f782-4ddf-b780-3a757cdb77e0",
    "7ae4d15d-62e2-459b-842a-446f921b9d3f",
    "7b0809fb-fd62-4733-8f40-74ceb04cbcac",
    "7b1f4ee4-7f50-4c82-9007-ba76528a84df",
    "7be20c8d-a23f-406e-8f03-aa9dfb4b30b1",
    "7c2c5cdc-80e6-49d5-8e95-08fc7da0a370",
    "7c3067c3-c229-4e73-b4aa-c187997ee530",
    "7c927849-94ed-4034-90e9-af34ac0cb47c",
    "7cc1fb18-45c1-499f-8476-682daa14a4a3",
    "7cdc1d6f-b047-4614-8931-c5849bb2b887",
    "7ce9b7d0-a8da-4528-bbe3-2c4f407f9cea",
    "7cea906d-ae65-420c-a6f7-a9a3ad64fb93",
    "7d47b53b-0a12-40e6-b57c-676021a2b18f",
    "7dd346d1-0425-4b48-a953-449560d8287b",
    "7e1daeee-a87b-46ad-a66f-268868c992c5",
    "7e27b76c-801c-41c8-a659-1dc07772bdb4",
    "7e78ee2b-3626-4dcf-a573-17768a466fc5",
    "7e9e452b-4335-4e86-8dcf-304e35ee08d3",
    "7eb70a9c-217b-4917-a156-78934f550bff",
    "7f497d81-4c7e-4e06-b166-a459968b14e3",
    "7fa09825-a2b5-462c-8f7d-d0765a083754",
    "7fcdca8e-7469-480c-8516-cce4e24c37c9",
    "8057906f-17c9-4e25-b173-4e7fb938078b",
    "8096525f-6f67-4bd2-a160-48ed4bea8aa7",
    "80c7c482-aa0f-4332-9249-179aaeb9d9c6",
    "80daac2f-e496-4c65-b196-6be7a9c4c98e",
    "80edb205-0f11-42f4-bc85-4ca5b3615b1c",
    "81316846-80cb-4913-8941-b31537761eb0",
    "8157bc94-5fba-4bf6-98bd-9ba653b595e8",
    "815e889f-3f34-4685-bfbf-f62d471d5fd7",
    "81843c4f-38cd-488e-867a-28665ad54b36",
    "81d00e23-92aa-45d7-b289-5cf045ddfbf4",
    "81dc7cdb-66be-4683-ae79-068a784378b1",
    "81e4458c-c368-414e-9bfa-69e2af48799a",
    "821c1855-6817-40ee-8732-7f472d238513",
    "822974ae-f202-43bd-a39a-73d56333d2fe",
    "82541f90-fe8e-4d66-84d8-4fe515dc5533",
    "826420a8-3d4a-4cee-901c-f0f2ee9e00b4",
    "82672123-feef-4b1c-9ee3-9a681204ae76",
    "8282ecae-6fba-4c4c-b393-cef07e6820b4",
    "82bcf2f2-b2d6-45a5-b0ca-d70d8ab4ebf8",
    "82c60e09-2939-4896-9ce9-5c63fe19cac9",
    "833306f7-91b6-4ff7-bc16-0e406334d991",
    "837d99c6-3045-4ba3-8951-643ddb3d6676",
    "83a4f2c7-7a52-46cc-a6c6-62b1aad5883d",
    "83ad8494-136b-485a-87d4-8ce01dd6a8de",
    "84006c59-fead-4b84-b3b5-cedf28f67ea9",
    "8445ab25-ff89-44b0-90f8-bf0790f50afc",
    "844875a9-9927-48a5-90b4-76c5f227f145",
    "845565c7-f4ca-478d-86f0-f7163ad60bb1",
    "8456dc3d-99e2-407c-ab55-4746d382496b",
    "84a9f073-9ba4-4c6e-a550-77d531e43472",
    "85ae9fb4-de87-41ce-abb3-44fda2fb24a8",
    "85e7ab74-5b32-4a14-ab1f-8c00f7168599",
    "85e930bf-6e90-4700-85d1-4c3330efbafb",
    "85f703d3-888c-403b-853e-10ea88ce935e",
    "8660ce9a-31c9-48ee-b5bc-9e6ba248ec0f",
    "8680bd09-df85-41cc-861e-dd33b6d04873",
    "86b1f54d-ac01-4c5e-8ed8-09da2689c7a9",
    "87017793-00dc-4f5d-b95b-09e7d17327cc",
    "8728ef71-fdcc-4027-8139-38b2c0628fba",
    "8783e947-93cf-4b60-b387-d10642b0eee0",
    "8784f0df-90ad-44f7-ab28-4b2d685640c8",
    "879d475f-4b76-4d18-8cf6-a7e5a6d44926",
    "87b32db7-7b5c-47eb-9d51-5d35ebbe81f4",
    "87c45c90-ba1d-409e-a9d7-9baf5a5cbb1c",
    "87fee729-2a4e-4d23-ad8a-5e03e1ab7c1a",
    "88595487-6d33-4980-ba54-bcf427c9466e",
    "88a30fd9-74d5-4931-a0f4-756b22772a0d",
    "88ad367d-2a11-40cc-8443-f9f00f5d760e",
    "88ec0fa4-1b6d-4420-bcc6-c335199001c8",
    "8919571f-205a-4aed-b9f2-96ccd0108e4c",
    "89320575-7d3e-4703-bfb4-aa1279db19e9",
    "8960fcec-05c0-43c5-a83e-87ace7d3091d",
    "89973ae1-54a1-45aa-8e5a-54a9f85c8726",
    "899855ee-4fd9-4e85-9033-880058303b5c",
    "89d715d4-8590-4482-93b8-246f80a1fbdb",
    "89eb1ad0-ae60-4e8a-bf34-a53d0423bc80",
    "89fe4242-59a2-4152-95b9-3c58b5880d69",
    "8a54c5fa-2900-4859-a2d6-1b7faedafac4",
    "8b0c1f4b-9de7-4521-98c2-983e0bfd33c7",
    "8b196eb7-1fbf-4eac-9f58-1fccae076f7f",
    "8b81fbbc-02d2-42ed-b06a-c04490cc32d1",
    "8c0d2c16-94d2-4608-9faf-5f79ee77c898",
    "8ca877bf-4f72-4037-8df1-0f1bdce0417c",
    "8d88dca6-355d-4ce2-af70-40cba4b2277b",
    "8dc14464-57b3-423e-8cb0-950ab8f36b6f",
    "8dd13cdf-1425-497d-a4ff-bb5dadfe21a8",
    "8e0b1e5f-be6b-4c4d-97a5-84c809fe8266",
    "8e4ff036-d38e-455f-a0fe-4ac50823f4fb",
    "8e5fffb5-0b22-472d-8386-de291d17d513",
    "8f237374-b220-40f5-8512-2adcf355b6b5",
    "8f5b2574-277f-48eb-91ff-efe075304a49",
    "8f689b8b-5b65-4638-9555-f2a5d237a624",
    "8fc08919-1137-42e4-9fa5-9e64f1e5757b",
    "90672ac2-5a99-47dd-bbd8-9808e5c90219",
    "90739622-5232-4048-8121-9af9ec69604f",
    "90e07356-df5d-4372-a2c4-34927db9a3ec",
    "90e622c7-f025-4f27-8891-82f0e867155f",
    "90f3bcb4-c0f1-49b9-8ba4-82a7440b3d96",
    "91481b2d-f93e-4eba-8fbe-7d6d47ae26ec",
    "91a0a18a-3196-4f87-87b6-02c7f8a12996",
    "91c5eec8-0cdc-4be2-9a99-a15ae5ec3edc",
    "91ee81ef-f00b-42e5-914a-fd26bd58ad5b",
    "91f4f1c9-37e3-430b-8020-f8d65af8e422",
    "929bf047-9ad7-48bd-88fa-c2630d423e8a",
    "92c44030-9b38-4433-9883-3d635fd63888",
    "92dd8c8e-c048-4f0a-9b5d-2ee627d2f553",
    "92e4e092-6dcb-46bc-85a0-dea8310aba45",
    "92f0b5e8-27b5-48ae-be72-616657821f7b",
    "9318b46e-efe6-4811-9046-98d87484a8eb",
    "931b72d6-1704-4bb9-9323-c4ac8207f3db",
    "93341fe7-38f8-4ef2-8dfc-ae550aa522dc",
    "935431f9-8320-470e-9eef-098f9284088f",
    "9354db6c-4019-4351-a822-cb87b1b73a44",
    "9368e302-f8e7-4714-aed4-db2faa861e5c",
    "93e97f6c-0ab6-41a7-9b58-7e230a80ec1e",
    "9471b5d8-99ab-474b-95c5-e4c3a3b71fb8",
    "948a3370-bdb4-46cb-a047-c777a76ae420",
    "94dd2cee-ed7d-4f98-894f-efafeac92b5b",
    "9510c293-746e-400e-8c03-26343430563e",
    "953b0329-c3e4-4816-a038-7afbd2bb2547",
    "954ec5e0-4fcd-414d-8ad2-46b4b75cfc74",
    "95773d4c-a441-410c-9fd1-aed26b41b0af",
    "95773ebb-2f5f-43f0-a652-bfd8d5f4707a",
    "959c0dc4-fcf3-477e-af63-c00a005dbc0a",
    "95ecb448-3c1f-4145-8565-4f6d51beb62c",
    "960e166f-ba48-4b90-b507-a3e770558b1f",
    "96588aed-3b7a-4179-b92b-2159427f4fcb",
    "96662fa9-ff60-495b-912a-284f3b98ed72",
    "96ce21fd-7c05-41be-a88d-43bda416a145",
    "97058091-eb35-401b-b286-18465761f832",
    "970741c5-4d7f-4fc8-8b88-0c9f37dff231",
    "97164605-d4b1-4b74-8cba-fddf834137e0",
    "9756b9a4-c070-4359-8a07-2383b09d0d04",
    "97ccd3cb-8d8d-4449-ad82-bfb31f614a2d",
    "97e4947d-fce9-4019-9f86-c0d94c820269",
    "981206b6-d45f-4534-9e8c-66f4d712a711",
    "984cbaa8-d801-4aa9-ad57-ec469f103a2f",
    "98714356-4005-4858-b2d8-c0ab8cf45c43",
    "98d8242b-f3f6-458a-9d68-86860c1ac671",
    "99097ebe-1efb-4df0-b314-5a9079776af2",
    "992d3eee-8098-4e87-97b1-d2cfb9508e7b",
    "994b60a2-88f1-49ec-a6da-27d56dfa6f16",
    "994e2360-35ed-43af-ad38-dba3a3fa513d",
    "995cc7f1-69c3-4317-ab77-28fd48f1e535",
    "99b04c9f-908e-42bd-92bc-41aa94b72949",
    "99c71f17-1446-4c38-a336-321fe6948dfc",
    "9a06cc34-be24-4ebf-b599-cbb1d4b8ac7b",
    "9a09745d-4449-46a2-b8b3-60f3c0d25e83",
    "9a1babdc-c3dd-43dc-b03b-33bbaa2725e7",
    "9a861ebe-f8d7-4eb1-a2c8-3006f07cfec2",
    "9ab47b07-99a9-4509-884b-be9383908b28",
    "9ace4c05-d930-45c7-8d2d-0cadff1ea32b",
    "9b27c2f1-8b0a-4482-8424-8a9bb3bf0cf9",
    "9b34b218-efb2-43b9-9b9e-dac3c470a9f9",
    "9b62118d-9b90-46b1-854d-06a5a9a22a90",
    "9b725e43-93c9-423b-adf8-a11d08a83d13",
    "9bd4ff72-1cb2-431f-bf7b-b5d47e08cc02",
    "9c5e903b-7d8b-4d06-825d-5857ab7d61b5",
    "9c963109-9898-4953-a351-d5ee36d6115b",
    "9cd2a269-be5a-4c8d-a782-994cf28fb431",
    "9d2a4189-6048-46e9-bac4-e5ef566334bb",
    "9d52a7c1-2e4e-4491-9dbd-153094e77f40",
    "9d8291f9-5c59-4630-8dea-4ac6cc0b1f78",
    "9d8ced48-62c5-4ce0-99e7-a03550c674c0",
    "9d9b81f1-3a9a-4515-b741-a145293f1fef",
    "9d9f63a0-29b2-4096-86fd-013c4b68e6e3",
    "9dcc3943-a32f-4b7b-9897-d9015965b797",
    "9dce915b-3de4-4a7d-a68d-e4c4c15809ce",
    "9dfecb79-7a51-4b66-9db2-f726479ea7d6",
    "9e046dad-2b23-4f95-8eaf-c0346de2556e",
    "9e103d5f-fc45-4375-b416-802659e6dc1b",
    "9e5aede6-bee5-4a3d-a255-513771b20035",
    "9e832050-5b37-48d8-8e66-06b6282eeaf9",
    "9e89b6af-4bb5-45af-93d8-0112bd20a60d",
    "9ef17d55-0498-44cf-9da4-dde3e3acb570",
    "9f5753ab-7ae3-45e3-873f-e35b18926796",
    "9ff03c57-ba5a-4127-9439-4bf3e838c4df",
    "a03da03d-0548-4aad-8038-cc8d9ff6e57d",
    "a0546df1-b727-402f-b9b8-570e65e58026",
    "a062eb42-d5c6-4332-8c88-64b4ac1af892",
    "a08ae849-ec58-4faf-8e99-284978a2413d",
    "a11357d1-73dc-4d1a-9590-1fd91bf6f506",
    "a16dc8d8-ff4a-4d62-a684-2937fb292b8d",
    "a18fdb6c-92c7-4a9f-92c6-78a6d31f9edc",
    "a1b2bdfd-00c7-4c31-8046-2c991ca777d0",
    "a1fe9d97-2319-4abb-96c9-34a8a57d814e",
    "a2352be4-1126-4504-ac86-c36234f123fa",
    "a253451e-717c-4baa-9536-ea78d869b3c1",
    "a26c9f48-fd5b-4a8f-9035-b8b79d582547",
    "a286ba29-d44c-4b6e-ab51-9d410d0d86bd",
    "a2a17035-1e6c-46df-9178-a610df825336",
    "a2a7754b-2346-496d-b681-eb754ef32b9e",
    "a2b36fdf-50bc-44ef-a6a4-ca6dc1dc148a",
    "a2beb85e-f2b8-4366-8b3b-e5c5cc117aaf",
    "a2d6766c-974d-43af-ab76-5d679eb8fa06",
    "a2f11cb7-2862-4d0d-aefd-47ea67efd8fd",
    "a3173073-2467-4365-9d32-17122929e27e",
    "a3af7e9f-a9d1-4b71-b6f1-50a87f816fe0",
    "a3b54f92-360f-49a9-ae3b-9f5361549ef9",
    "a3b77120-3770-46dd-ba47-6941eff848b3",
    "a3f99752-2aa0-433d-b11e-7dc006b29209",
    "a4008341-831a-408d-9463-0a6ed34bd021",
    "a4088dc3-bee7-4eed-aedb-92a29b3e0dbc",
    "a4378725-7967-47bc-aada-0220e02e1f96",
    "a4b888a2-94bf-4680-b912-84964a236c82",
    "a4da53b2-cc72-40e2-8d67-f3a70ed0b5c5",
    "a50e98bd-13e9-41fe-a5cc-aee4f240628b",
    "a51c7ff9-08dc-4492-9dca-b6c9a0105f77",
    "a549ceb3-f2ba-4eea-b597-94dd392c1914",
    "a5b1b714-7470-4635-805d-a0cdcc6a6a4b",
    "a5fdee09-34c4-48bc-99ff-a503c93a9d7e",
    "a5fe6f13-2121-41dd-a036-dae15546ad91",
    "a6743a43-b86a-4265-9521-fad3a24461a6",
    "a68df423-aae9-4f4b-8a42-a36124627a53",
    "a6966178-9495-4093-9cb2-dfed4acddde6",
    "a6e02b78-6fc6-4cb6-bb87-8d5a443f2c2a",
    "a6eee223-cf3b-4079-8bb2-b77dad8cae9d",
    "a6fcd5a2-831c-406f-8b2a-91c937d701c0",
    "a7020dbf-35fc-46e8-a441-c0a6b957193c",
    "a7228b3f-982a-4518-a761-b19b00e14844",
    "a748a0fe-a6ae-4ce7-b88f-4e4ec1dc080c",
    "a76527b9-9af6-4aaa-bbdf-86259f4aa1b9",
    "a777928b-e8ac-48da-93e0-272285b530e7",
    "a7fcc83c-7e38-40b5-90dd-b686a58221b1",
    "a81001fa-e3bc-4196-9e66-305600e22544",
    "a83151ae-e1db-4166-9dde-438f6544dca9",
    "a8413649-05d9-46da-b137-1317a709453f",
    "a864b24d-531c-4cd1-9875-57b390c6d3f0",
    "a89f64ab-8b3b-4267-b18a-3207d25a45ad",
    "a8add96d-651c-488e-8ca3-ed3f85c7a117",
    "a8ae97e3-ece0-4c43-b193-bdd61d724e2d",
    "a8f0fbaf-68b3-4e73-83d7-24d238ac966d",
    "a986ff36-22a2-46a5-ac82-513b6fa90423",
    "a9b8572c-ec86-4e6b-9ed9-03d939b7f363",
    "a9d3ea5f-7456-4ed0-9711-46caca3935a2",
    "a9fa92e7-d003-4549-802a-bb008613bec7",
    "aac5fd7f-8043-4aa8-811f-e50de70d96f3",
    "aad0f45b-521b-48ee-9039-68569710724e",
    "ab4b6a2b-a90a-44ce-95a1-2c44c911fcc6",
    "ab56632e-dbf5-493f-bcb1-0b77bcdfebfd",
    "ab820732-0844-4323-ba14-c58c24f3fc7e",
    "abb0a03c-4dcb-4f6f-a31d-55268f63d44c",
    "abbf4722-c63b-492f-b183-cb45ad9f5211",
    "abbfaa6a-d303-4916-af8d-7426f90a3983",
    "abe3903e-ceba-4864-aa5d-bd985c70fa21",
    "ac743418-6ef0-41c2-8f1d-ea15a5b22ff2",
    "ac888c29-a9d2-41dd-84a4-494feb010ae2",
    "ac8e80f9-1540-4dbf-b168-d8d0fb23a66b",
    "ac9e26a4-01d3-475e-a0c1-1ba4f5870047",
    "aced32f9-e511-48c7-8e9e-b625777bdf7f",
    "acf9885e-1e4f-4491-aee5-755cc77d5690",
    "ad098aa3-cf5b-49d8-97f9-eabdc88cfde6",
    "ad3198a5-3e39-4dd9-9d87-755a11b8e8fa",
    "ad54388f-ad6c-4b88-a718-4a25ad06a06f",
    "ad5c4ec7-ed56-4d3e-881a-963af217d334",
    "adae5c6c-72f3-4cd8-a00b-3ea71d516abc",
    "ae3510e6-b214-4d3b-80a5-72dbe01a6e5d",
    "ae56cfc8-8597-4a8e-b3f4-e8b0e521984c",
    "ae7c688d-2699-4d19-b168-905e0b90d314",
    "af40ed77-802c-484f-ba1d-680db571c9a1",
    "af4f13cd-f1e8-4e18-ac7a-b0aeee90c749",
    "af5c4ceb-3b3f-4bf2-a20d-b2ef32d6ed29",
    "af60ac71-fbfb-4648-95fb-0eb5fe24765b",
    "af7ddefe-35ee-44ab-8db4-4f981aaa6bd9",
    "afaeb2b4-0de2-417e-8206-d60e0c6999be",
    "afc645a5-9936-485b-961e-65e29582c228",
    "afe4d16a-cf6a-4be2-84ed-38c9bdc18c96",
    "b000920c-6f7d-49d3-9d0f-2bb630d2e01a",
    "b00cf471-6bbe-4f94-846e-288900398b65",
    "b027383d-6705-4d06-8514-db6ef16efdb5",
    "b092cd2f-18d9-4b4e-9ee6-226ad6a749bd",
    "b12b08da-3d05-4406-a051-0139a33ecf35",
    "b133b7cb-c0a1-4cd7-9775-cbc78fea50fc",
    "b1549ab7-5fc7-4966-a210-0846484fb171",
    "b17204ee-8a17-4ba1-9e74-bfadca6dce08",
    "b1c7a275-21f6-4b66-895d-d497359b34a1",
    "b2b294ed-1742-4479-b0c8-a8891fccd7eb",
    "b2e3ae4e-b490-4189-85ed-7eedcb24f3d1",
    "b3976394-a174-4ceb-8d64-3a435d66bde6",
    "b3d38693-5f9b-484d-aca3-6cefcc5b08e0",
    "b3d3a357-9fa6-453c-9f02-d86a1bbc762a",
    "b3d53973-5bac-432a-90d3-7956baa09c5d",
    "b3f10d7b-6763-4f79-9789-f6abc68b9720",
    "b40e13f7-a79a-4265-93d9-3b4878dfc988",
    "b4154ce7-8145-4fd8-92ed-edd124d53730",
    "b4bcc255-4acf-4966-b9b3-af9dd4e458d1",
    "b4cce5b5-6450-443c-8988-a279b9cefaab",
    "b4d4e884-a2ef-4967-b4cb-2072fc465eaf",
    "b4f21470-eae7-48b8-b46b-2ab73ad3b842",
    "b531ea59-025d-4c29-9d23-99ae75bcd55f",
    "b58043bc-9e47-4a8f-9e4b-5d4c510ea0e1",
    "b5d21367-a6c0-4d9f-ba77-74d70f335996",
    "b5d8168e-c310-4870-aa88-eeb3c25256fd",
    "b5e5c781-765f-4981-af2a-c19c250e2cf0",
    "b5f4526b-f4fb-4d90-8ce0-975e0cda8ff6",
    "b62cf6c0-e046-4c3f-9765-d0e046072b0f",
    "b6d0f953-29b4-41da-a255-2ed07c83edf1",
    "b6ebfa9d-fed3-4e0c-8877-47c1190f346c",
    "b6ec6203-09db-4d6e-8cba-ee4bebd2934c",
    "b7349341-c8e2-4628-be5f-77600ba730fa",
    "b74c1e30-f069-4783-9adf-fafe606d20e9",
    "b761d317-a36e-4a05-a5f4-bd3e3963daf6",
    "b803d3a7-34b4-447b-b239-2988a83181e7",
    "b85b071c-3b1a-4c6b-902f-b647ff8470e5",
    "b880344e-22b2-4540-a56a-1de5f5601a20",
    "b8972f6b-c67f-45c0-b348-954866e04a0f",
    "b8cbed64-5126-46bd-97aa-43627743aba7",
    "b91646be-9750-4052-8c0c-501c7c637ecc",
    "b9ab58cf-785e-44a7-a873-1966e14a6715",
    "b9d08bac-6b78-484b-9a96-da61552f53a5",
    "ba24d564-6c20-4faf-aeff-d3dfcf71767b",
    "ba54ba45-caac-4708-a389-ac94642976f8",
    "bb233277-6d17-4056-b462-a6511dfa6ba0",
    "bb7ec209-809a-4cac-8b0d-774d8da4ece3",
    "bbf5f8ed-f33f-40ba-9d0d-1c24dfec4193",
    "bc13ff8e-6f31-4f3d-b47a-38e3ce8a2194",
    "bc41afff-4d56-421e-bc87-45e18b4fe64a",
    "bc93e4c6-fd85-4412-987f-52f6fa3bb67d",
    "bcbae485-1286-4452-bbc9-bcb38c6c3573",
    "bcea3f80-ce61-4310-94b4-af6588410cbf",
    "bd61c458-b865-4b05-9f1f-735c49066e55",
    "bd6fb78f-3709-4282-a306-d865aed98b73",
    "bd7cfd55-bf55-46fc-878d-e6e11f574ccd",
    "bdf65f9c-a730-4083-bd8d-a2def3037637",
    "be31dfd1-c721-4697-8ee0-f7043c070810",
    "be34dbd9-5d54-4837-9f49-ff423eb18e8b",
    "beab5209-9628-4d4d-851e-2bc9bb1a0105",
    "beb74dc2-22ea-49e4-b1e3-bedb8e06e8f2",
    "beecd160-a96c-46fc-bdce-7dcb7024d473",
    "bf049384-ffe2-4418-a1a3-fc5552ba850f",
    "bf0e00ca-98a5-47b0-ad2d-26dc837b5939",
    "bf1fee2d-f760-4068-b8e6-d1db63ce434c",
    "bf5b0620-1ff0-45af-a2eb-96f3c739edf2",
    "bf897c17-06cc-48c1-a7cc-f41b45166880",
    "bf9066a2-2c5f-4cf2-821a-1a68b4df5b1b",
    "bfb53140-79c1-4625-81aa-3f37de7c0c2f",
    "bfc1bc5b-ddee-4880-a5d5-90f7c950c692",
    "bfdaaea8-99a6-4966-b9ba-905bdb0e32cf",
    "bfe01d36-2a16-46d6-857d-43fd4780dfec",
    "c0bb93c7-5906-43be-a2af-7f61078d5d7e",
    "c1122f57-9ab9-4552-9393-7d56b0bbe852",
    "c16e25c8-1e93-4571-881f-b757d3e48700",
    "c1cfa9ca-d0e9-4d2d-b0a9-528bd78d30c2",
    "c25d3777-e197-4ea7-a1db-50830976dce4",
    "c2767dde-1315-4d78-abf9-8e098dd588ab",
    "c282d7ee-b624-42a2-ace1-aff9bfff5e7c",
    "c2a8a955-8e20-4668-b676-bf58c2a0e7de",
    "c2b024fa-bcf2-4044-8cd0-53899cfbad28",
    "c2dcb184-6c90-4aa3-9ebb-33b2d53837b9",
    "c2e06358-1f9f-463c-843f-446c0a37fbd0",
    "c3134980-bf5c-49b8-a289-790d45f02c86",
    "c359c2e5-cd20-4057-9179-35a7a5b5da72",
    "c38b867b-05f3-4733-802e-d8d2d3324f84",
    "c3a7b339-122e-4fe9-8851-371b1fdf584e",
    "c4306d83-64da-4a45-bcb4-efd743caa2a3",
    "c481fbc6-4bd7-4c50-8537-ba1993d4eb88",
    "c49bc91d-0a50-497b-8b17-d77808745cf9",
    "c4bb08d4-c310-4879-abee-1b3986e8e0ca",
    "c4f0b43b-1a43-43bc-b302-2aecd37d8054",
    "c4f1ba85-b930-4456-a1ff-b6065dc990e2",
    "c50755ff-ca6d-4903-8e39-8b0e236c324f",
    "c530ad19-9847-4ea7-a807-f6753c3936d6",
    "c53bd7df-d2be-4a95-8b71-581c5b678d01",
    "c5418751-0901-48f9-a74b-481324764113",
    "c569e530-7322-40b8-9b66-1e0ed96fefcb",
    "c57817e7-034c-4796-ac47-2bc2191713b3",
    "c5916431-004f-465a-a505-589e2de29c8b",
    "c5b7dae8-b483-4742-9954-d7f9226daa34",
    "c5d42fed-eed0-4e14-9625-f8a9c0ff6bb1",
    "c5eeb223-0515-423a-a51a-151426c8f60d",
    "c60c8ac4-8894-46e3-8b89-3f1faf638311",
    "c6969e30-ca21-4576-954d-9c0e052bdde9",
    "c69f6868-4871-45e3-97a4-53645c92e386",
    "c6da100f-e808-4448-94da-5650232b47da",
    "c6e89321-fc23-4cba-ad79-be3e52edfb6d",
    "c70f7411-9256-4254-9af9-84a5211d650c",
    "c7158ac6-7e1d-446f-9801-b1af5e9cc1e3",
    "c75f2e8f-1f26-4cbb-b73b-7520b32a5b52",
    "c778d536-e9d4-422e-8e34-1d74a01f4c48",
    "c7821624-d246-43b8-9dfd-de470f9dc294",
    "c7a96a19-db06-48dc-bce7-5bf9bb4921a0",
    "c7ae0ade-c23e-4fe6-a3d4-79bd973374c2",
    "c804f67a-efed-4ff6-a875-dc132a040058",
    "c86e9646-1e6c-4bfa-984f-66e001f45f06",
    "c892f8d1-108c-44f7-9458-0abb96633976",
    "c8a2ddb9-fc62-4c3e-a296-41af3d5e8a60",
    "c8db3583-302b-4869-99b9-9ba23f72b8ac",
    "c8eeddef-b903-4aa3-a0fb-44344b8bf301",
    "c9008369-9652-420f-816c-eed7dd2cf754",
    "c9316f11-d955-4472-a276-6a26a6514590",
    "c94a06a5-df24-4546-adba-4f7940661826",
    "c96ca51c-908e-41cc-ae10-ac1fb72ca3d9",
    "c99c74cf-f13b-4128-bc5b-75aeea4be57d",
    "c9bf3a63-3b8c-47bc-a131-e74f0386cd47",
    "c9bf7d59-4819-4ca5-9ef1-cc1014f5890e",
    "c9bff84f-8de4-43e6-b195-f515f187a68a",
    "c9dad611-5e60-4456-934e-75b0e0842ddd",
    "ca356731-2be9-4e68-a475-0c363d12f54a",
    "ca46a6b1-3141-4881-a83e-0ac0fe079203",
    "ca4e7ee9-06f5-4f93-830c-507a6598ec25",
    "ca93c0e1-287b-464f-8db9-976071b4a401",
    "caaa464d-e290-4761-8550-75edc6d00119",
    "cab6d230-499b-4a21-9cc2-2750e14e92f8",
    "caf2eee6-83ac-4dae-aef3-e2fb3b06849f",
    "cb2973af-0d5a-4fbf-80ab-ec96ede33ef0",
    "cb2e732c-20a8-4037-9ab0-67a59faeafb8",
    "cb2fd1bd-2a69-4974-89f5-3f0b2f61276b",
    "cb33cf97-2a7b-4b45-9b73-5aca568332a6",
    "cb65cf5e-07b8-4d53-a91a-7dce9b8ccf80",
    "cb790bee-26da-40ed-94e0-d179618f9bd4",
    "cc11b4e3-823d-4490-95b2-afe6a1f3a9d2",
    "cc75bd05-251f-4cbb-afed-100cac9d7aa0",
    "ccba425a-25e0-4d94-8fde-3890be03ae1b",
    "ccd17772-d220-4088-8fa3-df3729f14df4",
    "ccda18a2-1473-445a-a66e-281b3b2968c6",
    "ccda5519-d34c-467f-8bc4-46d4edac16d2",
    "cd177f63-761b-44f6-866e-ee19d2ac134e",
    "cd46dc32-5e36-414b-a8d7-dea9df1f9106",
    "cd5bc13d-ee5c-4b68-a550-37edb3e7899d",
    "cd83554f-fca2-441e-8b71-89a5f7619a1b",
    "cd9e44d9-40cd-4a75-ab9c-4db2ac9e87ea",
    "ce0282d9-2858-4617-9bf2-a7581d54fb8b",
    "ce02a047-625d-4017-aeef-2c34e53824c8",
    "ce0c9488-97f0-432a-a0fa-d204230366be",
    "ce3e1545-2345-4e19-b6bc-068104bdba48",
    "ce86664d-2dfd-4280-8393-4a417231232c",
    "ce8d00be-3dcd-4d7e-9d03-f2915369dc16",
    "ce98b978-4542-45c6-aecf-79cf3f6979ed",
    "ced8c9bc-e8b5-49e7-860a-289fc913860c",
    "cf42855e-a54a-4488-a79e-beac086ba1d4",
    "cf60ed8a-2c79-4b85-a259-15a8e216dae4",
    "cf641fbf-fa31-481a-993b-9204f2ee1884",
    "cf7125c4-59b0-4b94-93a4-6a1772636816",
    "cfe5c00e-bec3-4120-a8eb-62c5353f3e80",
    "d0105f1d-a9a0-4cd4-817d-aebfb5512923",
    "d01e758a-e79b-4ecb-b0b4-469240be7b3a",
    "d047da74-eb5d-46e4-952d-1b374b4687d9",
    "d06a16c6-f540-40a8-9e92-876ad1955d03",
    "d07e7b8a-2222-477f-a7f3-f098bbfdaf54",
    "d0c0dd7d-fbe1-4e21-85dc-5662b6f26b2a",
    "d0c73947-fce1-4914-abf0-280584f89510",
    "d10bc322-69ed-4679-b113-a28ea6fe62fd",
    "d141ae12-cc66-426a-9f9b-f17e670464ae",
    "d1983d53-434a-436e-a698-3a2745eb61dc",
    "d1b25dcd-472e-4902-b53c-3b164269e049",
    "d1b72ebf-e69c-46cb-82a5-2e7d8ac4fd0a",
    "d1c20e8e-d875-473b-bfbf-fecf35ed00d0",
    "d1f70494-b5d9-4c84-973d-e34445b7552b",
    "d2217bca-3a93-4407-bb56-087afa000cbc",
    "d29268e4-cb4a-489f-a2db-a5b05a0ce6af",
    "d29b9265-07e6-4e73-8f72-fc42d3d83fb1",
    "d2c71720-e156-4943-8182-0a7bbe477a37",
    "d2c7cbb7-4102-44fe-bc40-7123c5425c5b",
    "d2e1d686-7e52-410a-8755-78e9f60376a7",
    "d315c4a3-0bee-49d1-8d03-726358937cde",
    "d3412433-4df9-4828-89e0-73956898f749",
    "d3579463-3a9b-4016-bc10-53b966ffb521",
    "d38fd1e8-bc15-46b2-92d5-5f3df98cff53",
    "d3920b32-8de2-4c92-a787-71497171595d",
    "d3e921c5-201e-4a59-a7fb-7450995add92",
    "d4573d80-0444-4391-a43b-165145c4464c",
    "d4c97b1f-cb56-4348-9072-e7313f0d66fc",
    "d55edb72-ed5a-4d59-ac23-6329f4c0c232",
    "d570c5b3-6198-4e78-93aa-2a2e2ec90d8b",
    "d5a1b706-c624-43df-afcf-9cea7094e75b",
    "d5c32031-231f-4213-b0f1-2dc4bbf711a0",
    "d5e327dd-5494-44a1-8184-598fc9c2f034",
    "d601bd5e-4f4c-4637-9196-4bd821dbd2e2",
    "d621e959-2633-4ec1-a2a2-5d97cd818b47",
    "d6224fb2-b12f-403b-ae8a-214e47541be8",
    "d66de9ed-f1c5-4228-8417-3a4934497e17",
    "d6ac6bcc-5473-46ae-a2b9-ca1813bc2438",
    "d6b13e38-15e2-4717-b337-ca1085948ede",
    "d6c6e57a-4ccb-4707-b87d-c91d01d6aa42",
    "d70edfe9-c358-4bf3-96d2-22fddc702994",
    "d767f759-af64-4464-8614-c77ca44cad8d",
    "d78c4f23-f6c4-42cd-8ddf-d5a98c351545",
    "d7ac2974-c14b-4c8b-9b4c-168506118b1b",
    "d7b285d4-2643-45ee-9302-b0c3d51dda5c",
    "d81c6ad6-fb8f-4c31-bba3-f2b65f780893",
    "d8862887-ff5c-4caa-9d61-f1958887ebc1",
    "d88dcf61-12fa-41b4-9f8b-bf4da1e68266",
    "d8b21973-76c8-42f7-88ac-bd533f68cbfd",
    "d922c684-9e2f-4a33-b5d3-8d811204071b",
    "d98bb5cb-698f-4ebc-aa3a-238d908e5474",
    "d99f352f-1fe5-47f0-be39-46ddd719f2f3",
    "d9d38b3f-5173-4051-98a6-2efad16fc8da",
    "da067ad9-b545-4701-aa3b-8d2990f8c1c0",
    "da1ef9b1-761b-4bc3-8ac0-b9a109101f5f",
    "da4681c3-64ba-4c99-8575-6cf0b2e55468",
    "da67ebd9-52de-444d-b114-e23c03111ac6",
    "dad2d007-d3e0-4c2e-a9ba-4d7d51749dd9",
    "db21017a-295c-47f1-805f-eaf57fefe18a",
    "db3181c9-48dd-489f-96ab-a5888f5a938c",
    "db4bb0df-8539-4617-ab5f-eb118aa3126b",
    "dbb14b3f-3a6b-4f3c-872d-9a5a28064a61",
    "dc50ab90-7f7f-4622-b93d-d4645fb67150",
    "dce13726-a7c4-40b0-8d73-7d8ddd0756d3",
    "dd232f5c-7f53-48ec-9bb7-7205702c3dc8",
    "dd783e7e-36d8-4fcd-b7fe-9a481d785560",
    "dd881b50-df17-48bd-94b5-0c07e1dfaa95",
    "dd91b2ab-b30d-4945-8f37-276b3af5fda6",
    "dddf9315-7819-4289-b587-6be72e4894d2",
    "dde625e8-cc2a-4877-9ec4-0b8a20dfded9",
    "de398955-e64b-43c6-ae32-1d95f3f581d2",
    "de41423b-7326-413a-880f-58176ef95ec7",
    "de56670c-2032-4833-9a89-46f7d6a037c7",
    "de764b86-6247-466f-a81b-7547ba390346",
    "de7bcb30-71ad-474d-aeed-ee384dc0bf04",
    "de9a1acc-2ca6-4c36-92af-e7ea8b29c096",
    "ded380b5-1ba2-4089-8e0c-0aa1b4140785",
    "deeae7f1-6016-47bc-afe3-2eb83e3af063",
    "defc5edd-4513-488b-9a7b-71919fe10190",
    "df22987f-d20d-41db-b8eb-8b5f5fca6df0",
    "df2db977-44f8-4dce-a728-ae4992b16d33",
    "df516dc6-6ef0-426d-94e3-8a2bbb0439a5",
    "df5b5ad2-0140-4581-8635-a420307d2d2d",
    "df6b3fa6-375e-41eb-8994-b7c5192d1032",
    "dfd53a42-8f63-4040-93a5-3f1347ce7686",
    "e01c69a9-c561-4e3d-8a2e-cfeeb4d9528e",
    "e01f53f2-8e1b-41cb-9e5a-3dfb3ccf44d6",
    "e0cb02de-3e8a-468c-9e5a-b509407b47db",
    "e0e37702-32af-405b-b652-ee54b5bb94e2",
    "e0ec0a71-25ad-472d-ab95-690bff2eedcf",
    "e0f49789-cdec-49c2-b32e-4a835b121e97",
    "e15b02ba-9593-4771-b584-bcc2b7868ce9",
    "e165b318-d5f7-40d5-a0d9-82ba3c31060f",
    "e1b03497-7632-4ba4-a9e0-dd230d06638c",
    "e1e0f2cc-a50c-40d4-9031-c2d90a826247",
    "e2066e9d-e5d1-440a-b43c-02cae16dc001",
    "e27f0218-47e0-41bc-9086-9d9169096e90",
    "e285de72-533c-43ea-baac-39e4d186febe",
    "e2a12ef3-cf03-4ce4-8aff-802ccf2aec1d",
    "e2c129bc-e45b-43d1-ab52-86e52093080b",
    "e2def7e2-1455-4856-9823-6d3738417d24",
    "e33a1faa-5175-45bc-89b8-3ec9cdd63cfb",
    "e34cf41b-196c-4199-85d5-4d2ca5954b09",
    "e36691ec-c4f8-4bec-b331-b48ffa82ff49",
    "e38af226-7109-4f99-a6f1-9fd3bec5638d",
    "e39f6dee-f2cf-4eff-afc9-4600cafe660c",
    "e3c3f4db-e4f3-4198-9c43-4d518d5893a3",
    "e3efdab9-2200-480c-8960-e163ee23dddf",
    "e3f5ac40-ec34-4423-9aa8-d660a65864af",
    "e40bcf8a-0d64-4d88-a02d-fa047b195e8b",
    "e452d0a5-19e5-40da-b7dd-817b2b9db7dd",
    "e4b33221-1e2c-405c-ac02-a39d93f9a69b",
    "e4c3e545-45fc-4aa6-958a-bd67124cfad8",
    "e4ff51ba-5007-4c40-9a86-e8c6f4db77b7",
    "e506c5e0-99b6-4e97-b7b4-4536cb80209b",
    "e53e1268-8af3-4221-9f7e-41199858bf18",
    "e5726cdc-dd31-45fa-a572-b0534181098f",
    "e5b0c46a-5eb6-4b94-9d4c-fb1000f534b0",
    "e5cf06b5-7d52-46ae-89b1-bb4e66c53ee1",
    "e5ecc0f9-26c0-42ee-a688-6a392070e97e",
    "e5f57bb0-07ec-4405-90b6-dc89647a1cb5",
    "e62ce6d3-ad76-4cbc-82bb-c5b45fe101c2",
    "e647814d-6975-4b34-b8c4-e79b7ca83085",
    "e64875de-88cc-43b0-8bef-37a1eee643cb",
    "e660150e-bfe1-46bf-a987-eb688e4f7164",
    "e6642584-cbe8-4d3d-806d-6a4242515742",
    "e6b4d68a-59b5-4b74-a5ce-daadd930d3ca",
    "e6c7d9d7-1550-4678-bae3-a4138bc48a9e",
    "e6ccc2bd-9451-4802-8a51-8640d9f09793",
    "e6d3c1da-a02f-43a2-a5ef-6a035298b933",
    "e6e6af0a-f258-4899-b3fc-d07e09f0fbc6",
    "e6eba8cd-fa2c-4ba2-bec0-6841e7633695",
    "e7016bd5-cb10-45b9-8959-0f5750f7a5db",
    "e701ecce-f9ab-445f-afcb-24f279efbc9c",
    "e703779d-89d8-43cb-8c71-69bd37acfa0a",
    "e7080e87-d140-4014-85ea-da02d70c6fa1",
    "e70af26a-fb9e-43ab-96a0-d62a2df37e6d",
    "e7301984-8b46-4932-b670-21c231ae4c01",
    "e73fedf0-90ee-4c6d-88dd-49399878fc54",
    "e7644bac-b532-41fb-92ae-1fb6502203f5",
    "e77108de-88dd-4931-8085-0ea59d7ca4ee",
    "e77a9815-9f35-4b6b-8a15-3daee6be6f1c",
    "e794b292-4a94-471e-ab84-6aaef1fea1b3",
    "e7ac34e5-ca8d-4e0c-b03f-e60073e30199",
    "e7ac8c4a-64bd-491b-b764-232de9b4bfe5",
    "e7bc6545-f14f-4fc4-9902-1aa38040f184",
    "e7db896a-c95b-4a18-99a4-866fff238ca5",
    "e7f38e53-a573-4e4e-b983-c732ef0c9970",
    "e812c193-89b5-4da8-980d-e759ac50ffba",
    "e85a9948-9c9e-451d-9485-b2b4cb7b73d5",
    "e87269d6-9460-4e85-aea3-c3a57c853ca7",
    "e881a3e2-f7ba-43c8-ae9a-11fcbfd741bb",
    "e8b9e1fd-7867-4013-9489-9941833c5b9d",
    "e8c034d5-c2c7-4f23-8dc0-7f94f4116306",
    "e8c1413c-4e2e-46d7-9b6e-df0e416e3786",
    "e8de3b7f-5783-4437-8ae6-31e402b3a8fe",
    "e910cca3-e695-4688-afe7-48c1902e6762",
    "e95396c4-1cac-4c9b-b461-5f21cd978fc6",
    "e9633eea-971a-48c6-b6f9-a98c5c12ec10",
    "e96d17ac-1a4c-47a1-9e69-77f48b7a7f5e",
    "e9b69a0a-497d-4201-b114-51519a4dfef9",
    "ea12da76-1b2e-4944-8709-1de3af1c65e2",
    "ea22b637-e43a-49be-836f-6d6d675b4e47",
    "ea44c7c9-9cca-4fae-9a0a-84f1b95f21cf",
    "ea4794f2-b034-464a-b178-e895d97bb15a",
    "ea9d9c05-11b3-4514-898e-66bd8560ec5b",
    "ea9de87b-7231-4a05-809f-4b658ea4173d",
    "eaa5f19e-ff6f-4d09-8b55-4a6810e77a6c",
    "ead34021-89fc-4bad-b0ac-a7fa94edd6b1",
    "eaed5ba6-aa32-4a57-a9e1-afc6b01d1c98",
    "eb03e611-4548-4b6d-ac65-d1487bcb873e",
    "eb119e1b-77c6-4801-a1f1-30cc0b005545",
    "eb526a6d-7f7b-49d9-9341-4e20e065059e",
    "eb5a1bf9-e758-4ea4-8db9-8e9d910c91de",
    "eb8caf28-4ddd-43e3-bd4c-6cdecbc9d573",
    "ebd2cc1c-6493-4e6a-bf56-8e563a1ed63b",
    "ec135c9b-ff89-4f28-aa18-88b16d932d94",
    "ec1ddb11-980c-419c-8b4a-db5ff717b0d7",
    "ec248223-f277-4c02-b1fa-60056b5a689a",
    "ec359278-df8e-4766-a1d3-4b55fd822704",
    "ec4289d8-1114-43b9-9f04-ad809aa1dbe3",
    "ec4acc26-ff1b-4131-a1df-a950fe31bb0e",
    "ed109ff0-2e4f-4a14-b130-d90e06756982",
    "edbd0bc4-c292-426b-9a6c-44bbccab2d11",
    "ede952f2-0182-4f43-bdbb-45fd08294db9",
    "ee0e3b9b-8d8e-4996-a102-1c2c016456ee",
    "ee8a365e-25fd-4505-a154-af6f290c127a",
    "ee9d38ef-c1db-44de-b8f2-62acb7049370",
    "ee9f6c4f-69e1-4199-b71f-5def6f865001",
    "eeb4872b-7fb4-4ecd-8ce5-82e194f04735",
    "eee4afe3-4ecb-423a-8aa9-eac809e20adb",
    "ef04e127-bb7d-4bf0-82d3-767d43108f81",
    "ef0a8f4a-32da-4e56-9d7e-80ec3217f74a",
    "ef30a918-b583-41f1-9ac4-4a37591b515a",
    "ef5e90cc-baf4-40a7-90af-579e4e6d0a11",
    "ef637c01-c551-4d47-8a48-4442b8ad5ecd",
    "ef684b6d-ba22-44af-8f66-75f9545dff48",
    "efc8b829-65b0-4fff-b6b2-f1148c68f80d",
    "f0174bc9-0cca-450e-a941-655d80040139",
    "f062cb7d-c03e-4762-b1c4-49118fee1a56",
    "f08c31ea-0e90-4cc1-b471-dfd0584ae7cf",
    "f0a9bca8-4767-4513-ab62-3f7374e469bb",
    "f0bb124f-5840-41ea-98ab-b8fd8802ea5f",
    "f145327c-e4aa-4a00-9b2f-8cec8416e605",
    "f14e84c3-85dd-4eb5-b195-7347f4ce8ca3",
    "f1960cd5-e27a-40f0-b4bd-3ca7157e4bbb",
    "f1a78c0f-449c-45fa-9472-0b92cc2a58da",
    "f1cf8457-237e-487a-9d13-5de7d81b9de4",
    "f22788e6-12c8-49af-a0ef-db5eca760b8e",
    "f266d3ab-5340-4f17-b211-2e90829cfb65",
    "f27a2f1d-1b62-424b-b73e-5fae8d609ed5",
    "f2e2deab-0763-403d-985b-155e3a3acc7c",
    "f2ea09aa-f47d-4d03-95b1-38f9a00eb214",
    "f2f68b10-b620-4ef8-ac32-c799b38b6d56",
    "f3395c1b-3221-450f-9e07-143ce74d29b3",
    "f33e9494-7b3c-4ac6-a735-59693b5a9638",
    "f363e1ff-f860-431b-89c5-3a0dc989a5cd",
    "f3650388-a713-4789-9105-ec4604c86be3",
    "f39780bd-7108-4685-8e6a-b340ff5a5965",
    "f3a5ec1a-49dd-4a52-8bd8-67cacae7a7ac",
    "f3d1fbbb-93d5-432e-8808-ebc08c42ef6d",
    "f3ee2661-268a-48dc-b931-b9429d5674f4",
    "f3ee8080-9a29-4f8b-ac51-441738366870",
    "f4138075-8edf-4629-952a-d9fb3b09d4e1",
    "f4214a7a-6793-48e0-ac41-9baff83096d3",
    "f42b0bf5-3e14-4a37-b209-3f7314273996",
    "f4a7278d-e60f-4da2-af99-ecd6cd9b69c3",
    "f4bec217-9676-4fc0-be90-856b4b89d4d1",
    "f4f833ea-0abf-4059-948e-132c64dda1be",
    "f5072c96-84c7-413e-b4ce-5530de191838",
    "f52fe755-e2a5-4073-a77b-5cb01275e4eb",
    "f56d3b76-c8c7-4280-969a-65fce63cff0b",
    "f5a7def2-1400-4b52-b756-eeb893041f7c",
    "f5c38252-89f1-4753-af3a-da8818fe3a86",
    "f5ef2de5-7a75-4a0d-9ea3-779320b8baf5",
    "f6298100-86ae-458f-9fb1-bbb3bb325422",
    "f630e3ea-697f-404a-8683-b86712c26c43",
    "f6420b5d-34c2-4f0e-9d7b-1a696e026311",
    "f778ecc0-8371-49d5-9ab1-9d75f0b76fad",
    "f77daa20-545c-4fce-812c-cdb4d658dfa5",
    "f83517ea-7b9f-443d-9371-d11a05ebc0a7",
    "f84d528a-7d08-467e-b532-ace707316f1d",
    "f85b922b-7b7e-4b0d-8f36-bdaa1b0043e8",
    "f885076c-a7ae-4a7b-9769-3f733c6a9ecf",
    "f8866892-56a0-4f46-9583-6719d42d81de",
    "f8e830e3-9b0d-4e5d-9f24-9f2ed3b465da",
    "f8eb4f5d-d392-4fd0-ae6c-c385dd0c4597",
    "f9714c75-a0df-439e-a18d-24e0f172b650",
    "f9a33279-d6ba-41c7-a511-ef6adfcb6e20",
    "fa0abbca-8c34-4579-8209-1c3b7e8bda7c",
    "fa1518c7-2cb3-49a3-98a6-67e0581b7669",
    "fa1cd665-255d-42d1-81be-a68a8870904a",
    "fa83c2bd-926c-42f1-8cd4-d57c5fd21f33",
    "faa0e463-0be9-427b-9674-1ac66e81ab98",
    "fb33ec4c-1bab-48f2-9faf-a5205a9a2c37",
    "fb4a4330-9124-4013-a0e1-af42ee20cd16",
    "fb7d5a61-6c48-4c64-88df-2202894b68c4",
    "fb7db48e-1599-420e-8f10-1f20fe1c6aa3",
    "fb97dfb4-72be-4dc1-9f5a-2faea75341b4",
    "fc014977-92f7-47fd-92d7-b609c39d8212",
    "fc40fabd-0a70-48fa-b142-79990cd259a5",
    "fc6210cb-34ef-4f38-841b-b5d3a74c3c34",
    "fc628e53-5fdf-4436-9782-bf637d812b48",
    "fc7b78d7-82d4-4648-8185-87a0ba209c20",
    "fcbcb214-cd62-4453-af56-b4b49161a261",
    "fccc3c1d-d9df-4ffd-b7e1-1b9eb11f95b1",
    "fd14095c-3658-4e00-8cec-729a89459e92",
    "fd44ca46-788c-4a61-a996-c316ae090886",
    "fd62a976-d195-492a-b6f0-f57fef8b6acc",
    "fd66cee6-5310-4201-9949-0ea04a05b72b",
    "fd8dae05-5c60-455e-ac8d-ace2649d6cbe",
    "fd9201e8-391d-4ab2-b7d4-b5dc39b0e995",
    "fdf7bb59-aad2-4f10-879f-6c0e7d3baa64",
    "fdf98e60-9feb-4b86-a42e-ae6c7152d02c",
    "fe04dab1-5a3d-4c28-a450-012658e982d8",
    "fe09c4c1-f6f9-421d-ad3d-d46795a2c399",
    "fe424bd8-a9f0-4292-83a5-a77279853317",
    "fe4a66c5-32f7-4e6c-b53b-c791663f1112",
    "fe51bced-93ce-45b2-b0c6-f7256719a07b",
    "fe52a17a-a7aa-4f95-a3ea-26fe640170fe",
    "fe87e9cd-02be-4722-9a22-effbb37e0ad6",
    "fe8baffd-c23d-4495-862a-e48912fe1f62",
    "ff0d1543-247c-4039-a8ff-cf4fc224c449",
    "ff111763-e72d-4f24-8914-b5b2dd94908c",
    "ffae5ae7-431e-4858-ac0d-1e511d8f8687",
]

# print "Summarizing Recordsets"
# for rs in recordsets:
#     print rs
#     res = es.search(index=source_index,doc_type="api",body={
#       "query": {
#         "term": {
#           "recordset_id": rs
#         }
#       },
#       "size": 0,
#       "aggs": {
#         "dh": {
#           "date_histogram": {
#             "field": "harvest_date",
#             "interval": "day"
#           },
#           "aggs": {
#             "rc": {
#               "avg": {
#                 "field": "records_count"
#               }
#             },
#             "mc": {
#               "avg": {
#                 "field": "mediarecords_count"
#               }
#             }
#           }
#         }
#       }
#     })

#     for b in res["aggregations"]["dh"]["buckets"]:
#         es.index(index=target_index,doc_type="api",body={
#             "records_count": b["rc"]["value"],
#             "mediarecords_count": b["mc"]["value"] or 0,
#             "harvest_date": b["key_as_string"],
#             "recordset_id": rs
#         })

# Copy only Digest and Search
logger.info("copy remaining indexes")
es.reindex(body={
  "source": {
    "index": source_index,
    "type": ["digest","search"]
  },
  "dest": {
    "index": target_index
  }
})
#resume_reindex(es,source_index,target_index,query={"query":{"terms":{"_type":["digest","search"]}}},target_client=es, chunk_size=10000)