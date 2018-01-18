from __future__ import division, absolute_import
from __future__ import print_function

# import itertools
# import functools
# import datetime
# import time
# import gc
# import math
# import signal
import logging

from idb.helpers.logging import idblogger, configure

from idb.postgres_backend.db import tombstone_etag
from idb.postgres_backend.db import PostgresDB


logger = idblogger.getChild('ingestion')
configure(logger=logger, stderr_level=logging.INFO)



    # # Note, a subtle distinction: The below query will index every
    # # _version_ of every record modified since the date it is thus
    # # imperative that the records are process in ascending modified
    # # order.  in practice, this is unlikely to index more than one
    # # record in a single run, but it is possible.
    # sql = """SELECT
    #         uuids.id as uuid,
    #         type,
    #         deleted,
    #         data_etag as etag,
    #         version,
    #         modified,
    #         parent,
    #         recordids,
    #         siblings,
    #         uuids_data.id as vid,
    #         data
    #     FROM uuids_data
    #     LEFT JOIN uuids
    #     ON uuids.id = uuids_data.uuids_id
    #     LEFT JOIN data
    #     ON data.etag = uuids_data.data_etag
    #     LEFT JOIN LATERAL (
    #         SELECT uuids_id, array_agg(identifier) as recordids
    #         FROM uuids_identifier
    #         WHERE uuids_id=uuids.id
    #         GROUP BY uuids_id
    #     ) as ids
    #     ON ids.uuids_id=uuids.id
    #     LEFT JOIN LATERAL (
    #         SELECT count(*) AS annotation_count
    #         FROM annotations
    #         WHERE uuids_id = uuids.id
    #     ) AS ac ON TRUE
    #     LEFT JOIN LATERAL (
    #         SELECT subject, json_object_agg(rel,array_agg) as siblings
    #         FROM (
    #             SELECT subject, rel, array_agg(object)
    #             FROM (
    #                 SELECT
    #                     r1 as subject,
    #                     type as rel,
    #                     r2 as object
    #                 FROM (
    #                     SELECT r1,r2
    #                     FROM uuids_siblings
    #                     UNION
    #                     SELECT r2,r1
    #                     FROM uuids_siblings
    #                 ) as rel_union
    #                 JOIN uuids
    #                 ON r2=id
    #                 WHERE uuids.deleted = false
    #             ) as rel_table
    #             WHERE subject=uuids.id
    #             GROUP BY subject, rel
    #         ) as rels
    #         GROUP BY subject
    #     ) as sibs
    #     ON sibs.subject=uuids.id
    #     WHERE type=%s and modified>%s
    #     ORDER BY modified ASC;
    #     """



#        sql = "SELECT * FROM idigbio_uuids_data WHERE uuid=%s and type=%s"





# def uuidsIter(uuid_l, ei, rc, typ, yield_record=False, children=False):
#     for rid in uuid_l:
#         if children:
#             logger.debug("Selecting children of %s.", rid)
#             sql = "SELECT * FROM idigbio_uuids_data WHERE parent=%s and type=%s"
#         else:
#             sql = "SELECT * FROM idigbio_uuids_data WHERE uuid=%s and type=%s"
#         params = (rid.strip(), typ[:-1])
#         results = apidbpool.fetchall(sql, params, cursor_factory=DictCursor)
#         for rec in results:
#             if yield_record:
#                 yield rec
#             else:
#                 yield index_record(ei, rc, typ, rec, do_index=False)


# def delete(ei, rc, no_index=False):
#     logger.info("Running deletes")

#     logger.info("Deleting records and recordset from recordset uuid %s", uuid_to_delete)

#     count = 0
#     sql = "SELECT id,type FROM uuids WHERE deleted=true"
#     results = apidbpool.fetchiter(sql, named=True, cursor_factory=DictCursor)
#     for r in results:
#         count += 1
#         if not no_index:
#             ei.es.delete(**{
#                 "index": ei.indexName,
#                 "doc_type": r["type"] + 's',
#                 "id": r["id"]
#             })

#         if count % 10000 == 0:
#             logger.info("%s", count)

#     logger.info("%s", count)
#     try:
#         ei.optimize()
#     except:
#         pass


def main():
#     import json
#     import os
    import argparse
#     from idb.corrections.record_corrector import RecordCorrector
    from idb.config import config

    parser = argparse.ArgumentParser(
        description='Delete records and recordset by specifying the recordset uuid')

    parser.add_argument('-u', '--uuid', dest='uuid_to_delete', nargs='+',
                        type=str, default=[])
    parser.add_argument('--uuid-file', dest='uuid_file',
                        type=str, default=None)

    args = parser.parse_args()
    print ("Hello world")
    print (format(args))


#         if args.continuous:
#             continuous_incremental(ei, rc)
#         elif args.incremental:
#             incremental(ei, rc, no_index=args.no_index)
#         else:
#            parser.print_help()


if __name__ == "__main__":
    main()


