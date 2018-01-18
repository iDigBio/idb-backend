from __future__ import division, absolute_import
from __future__ import print_function

import logging

from uuid import UUID

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


def check_uuid(uuid):
    """
    Check to see if a string is a valid uuid representation.
    """
    try:
        uuid_obj = UUID(uuid)
        return True
    except:
        print ("'{0}' does not appear to be a UUID.".format(uuid))
        return False

def delete_recordset(uuid):
    """
    Deletes a recordset and all child records by marking them deleted in the database
    with the tombstone flag.
    """

    logger.info("Deleting records and recordset from recordset uuid '{0}'".format(uuid))

    pass

#     sql = "SELECT id,type FROM uuids WHERE deleted=true"
#     deleted_record_count = 
#

#        sql = "SELECT * FROM idigbio_uuids_data WHERE uuid=%s and type=%s"

#             sql = "SELECT * FROM idigbio_uuids_data WHERE uuid=%s and type=%s"

#     logger.info("Deleted %s records from %s", deleted_record_count, uuid)


def main():
    import argparse
    from idb.config import config

    parser = argparse.ArgumentParser(
        description='Delete records and recordset by specifying the recordset uuid')

    parse_group = parser.add_mutually_exclusive_group(required=True)

    parse_group.add_argument('-u', '--uuid', dest='uuid_to_delete',
                        type=str, default=None)
    parse_group.add_argument('--uuid-file', dest='uuid_file',
                        type=str, default=None)

    args = parser.parse_args()

    if args.uuid_file:
        # read each line in uuid_file as a uuid
        print ("Reading uuid_file '{0}'".format(args.uuid_file))
        with open(args.uuid_file) as f:               
            for uuid_string in f:
                uuid = uuid_string.strip()
                print ("Delete recordset and child records for uuid '{0}'".format(uuid))
                if check_uuid(uuid):
                    delete_recordset(uuid)
    else:
        # delete a single uuid
        print ("Delete recordset and child records for uuid '{0}'".format(args.uuid_to_delete))
        if check_uuid(args.uuid_to_delete):
            delete_recordset(args.uuid_to_delete)


if __name__ == "__main__":
    main()


