

from datetime import datetime

from idb import config
from idb.postgres_backend import apidbpool
from idb.helpers.logging import idblogger

logger = idblogger.getChild('migrate')


def migrate():
    """Migrate objects from the old media API

    Specifically the `idb_object_keys` into the new `media` and `objects`
    """
    t1 = datetime.now()
    logger.info("Checking for objects in the old media api")
    try:
        sql = """INSERT INTO objects (bucket, etag)
              (SELECT DISTINCT
                type,
                etag
              FROM idb_object_keys
              LEFT JOIN objects USING (etag)
              WHERE objects.etag IS NULL
                AND idb_object_keys.user_uuid <> %s);
        """
        rc = apidbpool.execute(sql, (config.IDB_UUID,))
        logger.info("Objects Migrated: %s", rc)
        sql = """INSERT INTO media (url, type, owner, last_status, last_check)
              (SELECT
                idb_object_keys.lookup_key,
                idb_object_keys.type,
                idb_object_keys.user_uuid::uuid,
                200,
                now()
              FROM idb_object_keys
              LEFT JOIN media ON lookup_key = url
              WHERE media.url IS NULL
                AND idb_object_keys.user_uuid <> %s);
        """
        rc = apidbpool.execute(sql, (config.IDB_UUID,))
        logger.info("Media Migrated: %s", rc)
        sql = """
            INSERT INTO media_objects (url, etag, modified)
              (SELECT
                idb_object_keys.lookup_key,
                idb_object_keys.etag,
                idb_object_keys.date_modified
              FROM idb_object_keys
              JOIN media ON idb_object_keys.lookup_key = media.url
              JOIN objects ON idb_object_keys.etag = objects.etag
              LEFT JOIN media_objects ON lookup_key = media.url
                    AND media_objects.etag = idb_object_keys.etag
              WHERE media_objects.url IS NULL
                AND idb_object_keys.user_uuid <> %s)
        """
        rc = apidbpool.execute(sql, (config.IDB_UUID,))
        logger.info("Media Objects Migrated: %s in %ss", rc, (datetime.now() - t1))
    except Exception:
        logger.exception("Failed migrating from old media api")
