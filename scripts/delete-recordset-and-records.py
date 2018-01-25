from __future__ import division, absolute_import
from __future__ import print_function

import logging

from uuid import UUID

from idb.helpers.logging import idblogger, configure

from idb.postgres_backend.db import tombstone_etag
from idb.postgres_backend.db import PostgresDB


logger = idblogger.getChild('ingestion')
configure(logger=logger, stderr_level=logging.INFO)


def check_uuid(uuid):
    """
    Check to see if a string is a valid uuid representation.
    """
    try:
        uuid_obj = UUID(uuid)
        return True
    except:
        logger.error("'{0}' does not appear to be a UUID.".format(uuid))
        return False

def delete_recordset(uuid, db):
    """
    Deletes a recordset and all child records by marking them deleted in the database
    with the tombstone flag.
    """

    logger.info("Deleting records and recordset from recordset uuid '{0}'".format(uuid))

    # * Five Steps to delete a recordset and records *

    # 1. set ingest = false on the recordset
    sql = """
          UPDATE recordsets SET ingest = false WHERE ingest = true AND uuid = '%s';
    """ % uuid
    db.execute(sql, uuid)


    # 2. tombstone the records
    sql = """
          INSERT INTO uuids_data (uuids_id,data_etag,version)
          SELECT uuids_id,'%s' AS
          data_etag,version+1 AS version FROM uuids LEFT JOIN LATERAL ( SELECT *
          FROM uuids_data WHERE uuids.id=uuids_data.uuids_id ORDER BY MODIFIED
          DESC LIMIT 1) AS latest ON latest.uuids_id=uuids.id WHERE parent = '%s'
          and data_etag != '%s';
    """ % (tombstone_etag, uuid, tombstone_etag)
    tombstone_count = db.execute(sql, uuid)


    # 3. mark the records deleted
    sql = """
          UPDATE uuids SET deleted=true WHERE parent = '%s' AND deleted=false;
    """ % uuid
    deleted_count = db.execute(sql,uuid)


    # 4. tombstone the recordset
    sql = """
          INSERT INTO uuids_data (uuids_id,data_etag,version)
          SELECT uuids_id,'%s' AS
          data_etag,version+1 AS version FROM uuids LEFT JOIN LATERAL ( SELECT *
          FROM uuids_data WHERE uuids.id=uuids_data.uuids_id ORDER BY MODIFIED
          DESC LIMIT 1) AS latest ON latest.uuids_id=uuids.id WHERE uuids.id= '%s'
          and data_etag != '%s';
    """ % (tombstone_etag, uuid, tombstone_etag)
    db.execute(sql,uuid)


    # 5. mark the recordset deleted
    sql = """
          UPDATE uuids SET deleted=true WHERE id= '%s' and deleted=false;
    """ % uuid
    db.execute(sql,uuid)

    db.commit()
    logger.info("Tombstoned {0} records from '{1}'.".format(tombstone_count, uuid))
    logger.info("Marked {0} records as deleted from '{1}'.".format(deleted_count, uuid))



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

    db = PostgresDB()

    if args.uuid_file:
        # read each line in uuid_file as a uuid
        logger.info("Reading uuid_file '{0}'".format(args.uuid_file))
        with open(args.uuid_file) as f:               
            for uuid_string in f:
                uuid = uuid_string.strip()
                if check_uuid(uuid):
                    delete_recordset(uuid, db)
    else:
        # operate on a single uuid
        if check_uuid(args.uuid_to_delete):
            delete_recordset(args.uuid_to_delete, db)


if __name__ == "__main__":
    main()


