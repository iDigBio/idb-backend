from __future__ import division, absolute_import, print_function
import uuid
import json
import os
import re
import sys
import itertools

from datetime import datetime
from cStringIO import StringIO

from psycopg2.extras import DictCursor, NamedTupleCursor
from psycopg2.extensions import cursor
from psycopg2.extensions import (ISOLATION_LEVEL_READ_COMMITTED,
                                 ISOLATION_LEVEL_AUTOCOMMIT,
                                 TRANSACTION_STATUS_IDLE)
from idb import config
from idb.helpers.logging import idblogger as logger
from idb.postgres_backend import apidbpool
from idb.helpers.etags import calcEtag, calcFileHash
from idb.helpers.media_validation import validate, EtagMismatchError
from idb.helpers.conversions import get_accessuri


TEST_SIZE = 10000
TEST_COUNT = 10

#tombstone_etag = calcEtag({"deleted":True})
tombstone_etag = "9a4e35834eb80d9af64bcd07ed996b9ec0e60d92"


class PostgresDB(object):
    __join_uuids_etags_latest_version = """
        LEFT JOIN LATERAL (
            SELECT * FROM uuids_data
            WHERE uuids_id=uuids.id
            ORDER BY modified DESC
            LIMIT 1
        ) AS latest ON true
    """

    __join_uuids_etags_all_versions = """
            LEFT JOIN uuids_data as latest ON latest.uuids_id=uuids.id
    """

    __join_uuids_identifiers = """
        LEFT JOIN LATERAL (
            SELECT array_agg(identifier) as recordids
            FROM uuids_identifier
            WHERE uuids_id=uuids.id
        ) as ids ON true
    """

    __join_uuids_siblings = """
        LEFT JOIN LATERAL (
             SELECT json_object_agg(rel,array_agg) as siblings
             FROM (
                SELECT type as rel, array_agg(r2)
                FROM (
                    SELECT r1,r2 FROM uuids_siblings
                    UNION
                    SELECT r2,r1 FROM uuids_siblings
                ) as rel_union
                JOIN uuids as sibs ON r2=id
                WHERE sibs.deleted = false and r1 = uuids.id
                GROUP BY type
            ) as rels
        ) as sibs ON true
    """

    __join_uuids_data = """
        LEFT JOIN data
        ON data_etag = etag
    """

    __item_master_query_from = """FROM uuids
    """ + \
        __join_uuids_etags_latest_version + \
        __join_uuids_identifiers + \
        __join_uuids_siblings

    __columns_master_query = """ SELECT
            uuids.id as uuid,
            type,
            deleted,
            data_etag as etag,
            version,
            modified,
            parent,
            recordids,
            siblings,
            latest.id as vid
    """

    __columns_master_query_data = __columns_master_query + \
        """,
            data
    """

    __item_master_query = __columns_master_query + __item_master_query_from

    __item_master_query_data = __columns_master_query_data + \
        __item_master_query_from + \
        __join_uuids_data

    _upsert_uuid_query = """INSERT INTO uuids (id,type,parent)
        SELECT %(uuid)s, %(type)s, %(parent)s WHERE NOT EXISTS (
            SELECT 1 FROM uuids WHERE id=%(uuid)s
        )
    """

    _upsert_data_query = """INSERT INTO data (etag,data)
        SELECT %(etag)s, %(data)s WHERE NOT EXISTS (
            SELECT 1 FROM data WHERE etag=%(etag)s
        )
    """

    _upsert_uuid_data_query = """WITH v AS (
            SELECT * FROM (
                SELECT data_etag, version, modified FROM uuids_data WHERE uuids_id=%(uuid)s
                UNION
                SELECT NULL as data_etag, -1 as version, NULL as modified
            ) as sq ORDER BY modified DESC NULLS LAST LIMIT 1
        )
        INSERT INTO uuids_data (uuids_id,data_etag,version)
            SELECT %(uuid)s, %(etag)s, v.version+1 FROM v WHERE NOT EXISTS (
                SELECT 1 FROM uuids_data WHERE uuids_id=%(uuid)s AND data_etag=%(etag)s AND version=v.version
            )
    """

    _upsert_uuid_id_query = """INSERT INTO uuids_identifier (uuids_id, identifier)
        SELECT %(uuid)s, %(id)s WHERE NOT EXISTS (
            SELECT 1 FROM uuids_identifier WHERE identifier=%(id)s
        )
    """

    _upsert_uuid_sibling_query = """INSERT INTO uuids_siblings (r1,r2)
        SELECT %(uuid)s, %(sibling)s WHERE NOT EXISTS (
            SELECT 1 FROM uuids_siblings WHERE (r1=%(uuid)s and r2=%(sibling)s) or (r2=%(uuid)s and r1=%(sibling)s)
        )
    """

    _pool = None
    conn = None

    def __init__(self, pool=None):
        if pool:
            self._pool = pool
        else:
            self._pool = apidbpool
        # Generic reusable cursor for normal ops
        self.conn = self._pool.get()

    def __del__(self):
        self.close()

    def __enter__(self):
        return self

    def __exit__(self, type, value, traceback):
        self.close()

    def close(self):
        if self.conn:
            self._pool.put(self.conn)
            self.conn = None

    def fetchone(self, *args, **kwargs):
        with self.cursor(**kwargs) as cur:
            cur.execute(*args)
            return cur.fetchone()

    def fetchall(self, *args, **kwargs):
        with self.cursor(**kwargs) as cur:
            cur.execute(*args)
            return cur.fetchall()

    def execute(self, *args, **kwargs):
        with self.cursor(**kwargs) as cur:
            cur.execute(*args)
            return cur.rowcount

    def executemany(self, *args, **kwargs):
        with self.cursor(**kwargs) as cur:
            cur.executemany(*args)
            return cur.rowcount

    def commit(self):
        self.conn.commit()

    def rollback(self):
        self.conn.rollback()

    def cursor(self, ss=False, **kwargs):
        if kwargs.pop('named', False) is True:
            kwargs['name'] == str(uuid.uuid4())

        kwargs.setdefault('cursor_factory', DictCursor)
        return self.conn.cursor(**kwargs)

    def mogrify(self, *args, **kwargs):
        with self.cursor(**kwargs) as cur:
            return cur.mogrify(*args)


    def drop_schema(self):
        raise Exception("I can't let you do that, dave.")
        #     self.execute("DROP VIEW IF EXISTS idigbio_uuids_new")
        #     self.execute("DROP VIEW IF EXISTS idigbio_uuids_data")
        #     self.execute("DROP VIEW IF EXISTS idigbio_relations")
        #     self.execute("DROP TABLE IF EXISTS uuids_siblings")
        #     self.execute("DROP TABLE IF EXISTS uuids_identifier")
        #     self.execute("DROP TABLE IF EXISTS uuids_data")
        #     self.execute("DROP TABLE IF EXISTS uuids")
        #     self.execute("DROP TABLE IF EXISTS data")

    def create_views(self):
        self.execute(
            "CREATE OR REPLACE VIEW idigbio_uuids_new AS" + self.__item_master_query)
        self.execute(
            "CREATE OR REPLACE VIEW idigbio_uuids_data AS" + self.__item_master_query_data)

        self.execute("""CREATE OR REPLACE VIEW idigbio_relations AS
            SELECT
                r1 as subject,
                type as rel,
                r2 as object
            FROM (
                SELECT r1,r2
                FROM uuids_siblings
                UNION
                SELECT r2,r1
                FROM uuids_siblings
            ) as a
            JOIN uuids
            ON r2=id
        """)

    def create_schema(self):

        self.execute("""CREATE TABLE IF NOT EXISTS uuids (
            id uuid NOT NULL PRIMARY KEY,
            type varchar(50) NOT NULL,
            parent uuid,
            deleted boolean NOT NULL DEFAULT false
        )""")

        self.execute("""CREATE TABLE IF NOT EXISTS data (
            etag varchar(41) NOT NULL PRIMARY KEY,
            data jsonb
        )""")

        self.execute("""CREATE TABLE IF NOT EXISTS uuids_data (
            id bigserial NOT NULL PRIMARY KEY,
            uuids_id uuid NOT NULL REFERENCES uuids(id),
            data_etag varchar(41) NOT NULL REFERENCES data(etag),
            modified timestamp NOT NULL DEFAULT now(),
            version int NOT NULL DEFAULT 0
        )""")

        self.execute("""CREATE TABLE IF NOT EXISTS uuids_identifier (
            id bigserial NOT NULL PRIMARY KEY,
            identifier text NOT NULL UNIQUE,
            uuids_id uuid NOT NULL REFERENCES uuids(id)
        )""")

        self.execute("""CREATE TABLE IF NOT EXISTS uuids_siblings (
            id bigserial NOT NULL PRIMARY KEY,
            r1 uuid NOT NULL REFERENCES uuids(id),
            r2 uuid NOT NULL REFERENCES uuids(id)
        )""")

        self.execute(
            "CREATE INDEX uuids_data_uuids_id ON uuids_data (uuids_id)")
        self.execute(
            "CREATE INDEX uuids_data_modified ON uuids_data (modified)")
        self.execute(
            "CREATE INDEX uuids_data_version ON uuids_data (version)")
        self.execute("CREATE INDEX uuids_deleted ON uuids (deleted)")
        self.execute("CREATE INDEX uuids_parent ON uuids (parent)")
        self.execute("CREATE INDEX uuids_type ON uuids (type)")
        self.execute(
            "CREATE INDEX uuids_siblings_r1 ON uuids_siblings (r1)")
        self.execute(
            "CREATE INDEX uuids_siblings_r2 ON uuids_siblings (r2)")
        self.execute(
            "CREATE INDEX uuids_identifier_uuids_id ON uuids_identifier (uuids_id)")

        self.create_views()

    def get_item(self, u, version=None):
        if version is not None:
            # Fetch by version ignores the deleted flag
            if version == "all":
                sql = (self.__columns_master_query_data +
                       """ FROM uuids """ +
                       self.__join_uuids_etags_all_versions +
                       self.__join_uuids_identifiers +
                       self.__join_uuids_siblings +
                       self.__join_uuids_data +
                       """
                    WHERE uuids.id=%s
                    ORDER BY version ASC
                """, (u,))
                return self.fetchall(*sql)
            else:
                # Fetch by version ignores the deleted flag
                sql = (self.__columns_master_query_data +
                       """ FROM uuids """ +
                       self.__join_uuids_etags_all_versions +
                       self.__join_uuids_identifiers +
                       self.__join_uuids_siblings +
                       self.__join_uuids_data +
                       "\nWHERE uuids.id=%s and version=%s", (u, version))
                return self.fetchone(*sql)
        else:
            sql = (self.__item_master_query_data +
                   "\nWHERE deleted=false and uuids.id=%s", (u,))
            return self.fetchone(*sql)

    def delete_item(self, u):
        rc1 = self._upsert_uuid_data(u, tombstone_etag)
        rc2 = self.execute("UPDATE uuids SET deleted=true WHERE id=%s", (u,))
        return rc2

    def undelete_item(self, u):
        # Needs to be accompanied by a corresponding version insertion to obsolete the tombstone
        self.execute("UPDATE uuids SET deleted=false WHERE id=%s", (u,))

    def get_type_list(self, t, limit=100, offset=0, data=False):
        if data:
            if limit is not None:
                sql = (self.__item_master_query_data + """
                    WHERE deleted=false and type=%s
                    ORDER BY uuid
                    LIMIT %s OFFSET %s""", (t, limit, offset))
            else:
                sql = (self.__item_master_query_data + """
                    WHERE deleted=false and type=%s
                    ORDER BY uuid""", (t,))
        else:
            if limit is not None:
                sql = (self.__item_master_query + """
                    WHERE deleted=false and type=%s
                    ORDER BY uuid
                    LIMIT %s OFFSET %s""", (t, limit, offset))
            else:
                sql = (self.__item_master_query + """
                    WHERE deleted=false and type=%s
                    ORDER BY uuid""", (t,))
        logger.info(self.mogrify(*sql))
        return self._pool.fetchiter(*sql)

    def get_type_count(self, t):
        sql = ("""SELECT count(*) as count
                  FROM uuids
                  WHERE deleted=false and type=%s""", (t,))
        return self._pool.fetchone(*sql)[0]

    def get_children_list(self, u, t, limit=100, offset=0, data=False, cursor_factory=DictCursor):
        sql = None
        if data:
            if limit is not None:
                sql = (self.__item_master_query_data + """
                    WHERE deleted=false and type=%s and parent=%s
                    ORDER BY UUID
                    LIMIT %s OFFSET %s
                """, (t, u, limit, offset))
            else:
                sql = (self.__item_master_query_data + """
                    WHERE deleted=false and type=%s and parent=%s
                    ORDER BY uuid
                """, (t, u))
        else:
            if limit is not None:
                sql = (self.__item_master_query + """
                    WHERE deleted=false and type=%s and parent=%s
                    ORDER BY uuid
                    LIMIT %s OFFSET %s
                """, (t, u, limit, offset))
            else:
                sql = (self.__item_master_query + """
                    WHERE deleted=false and type=%s and parent=%s
                    ORDER BY uuid
                """, (t, u))
        return self._pool.fetchiter(*sql, named=True, cursor_factory=cursor_factory)

    def get_children_count(self, u, t):
        sql = ("""SELECT count(*) as count
                  FROM uuids
                  WHERE deleted=false and type=%s and parent=%s
        """, (t, u))
        return self._pool.fetchone(*sql)[0]

    def _id_precheck(self, u, ids):
        rows = self.fetchall("""SELECT DISTINCT uuids_id
            FROM uuids_identifier
            WHERE uuids_id=%s OR identifier = ANY(%s)
        """, (u, ids))
        return len(rows) <= 1

    def get_uuid(self, ids):
        sql = """SELECT DISTINCT uuids_id, parent, deleted
            FROM uuids_identifier
            JOIN uuids ON uuids.id = uuids_identifier.uuids_id
            WHERE identifier = ANY(%s)
        """

        results = self.fetchall(sql, (ids,), cursor_factory=cursor)
        if len(results) == 0:
            return (str(uuid.uuid4()), None, False)
        elif len(results) == 1:
            return results[0]
        else:
            raise ValueError("Identifiers have multiple uuids:", ids)

    def set_record(self, u, t, p, d, ids, siblings):
        try:
            assert self._id_precheck(u, ids)
            e = calcEtag(d)
            self._upsert_uuid(u, t, p)
            self._upsert_data(e, d)
            self._upsert_uuid_data(u, e)
            self._upsert_uuid_id_l([(u, i) for i in ids])
            self._upsert_uuid_sibling_l(
                [(u, s) for s in siblings])
        except AssertionError:
            print(u, t, ids)
            raise
        except:
            print(u, t, ids)
            self.rollback()
            raise

    def set_records(self, record_list):
        try:
            for u, t, p, d, ids, siblings in record_list:
                assert self._id_precheck(u, ids)
                e = calcEtag(d)
                self._upsert_uuid(u, t, p)
                self._upsert_data(e, d)
                self._upsert_uuid_data(u, e)
                self._upsert_uuid_id_l([(u, i) for i in ids])
                self._upsert_uuid_sibling_l(
                    [(u, s) for s in siblings])
        except:
            e = sys.exc_info()
            self.rollback()
            raise

    # UUID

    def _upsert_uuid(self, u, t, p):
        self.execute(self._upsert_uuid_query, {
            "uuid": u,
            "type": t,
            "parent": p
        })

    def _upsert_uuid_l(self, utpl):
        self.executemany(self._upsert_uuid_query, [
            {
                "uuid": u,
                "type": t,
                "parent": p
            } for u, t, p in utpl
        ])

    # DATA
    def _upsert_data(self, e, d):
        self.execute(self._upsert_data_query, {
            "etag": e,
            "data": json.dumps(d)
        })

    def _upsert_data_l(self, edl):
        self.executemany(self._upsert_data_query, [
            {
                "etag": e,
                "data": json.dumps(d)
            } for e, d in edl
        ])

    # ETAGS ONLY
    def _upsert_etag_l(self, el):
        self.executemany("""INSERT INTO data (etag)
            SELECT %(etag)s as etag WHERE NOT EXISTS (
                SELECT 1 FROM data WHERE etag=%(etag)s
            )
        """, [{"etag": e} for e in el])

    # UUID DATA
    def _upsert_uuid_data(self, u, e):
        return self.execute(self._upsert_uuid_data_query, {
            "uuid": u,
            "etag": e
        })

    def _upsert_uuid_data_l(self, uel):
        self.executemany(self._upsert_uuid_data_query, [
            {
                "uuid": u,
                "etag": e
            } for u, e in uel
        ])

    # UUID ID
    def _upsert_uuid_id(self, u, i):
        self.execute(self._upsert_uuid_id_query, {
            "uuid": u,
            "id": i
        })

    def _upsert_uuid_id_l(self, uil):
        self.executemany(self._upsert_uuid_id_query, [
            {
                "uuid": u,
                "id": i
            } for u, i in uil
        ])

    # UUID ID
    def _upsert_uuid_sibling(self, u, s):
        self.execute(self._upsert_uuid_sibling_query, {
            "uuid": sorted([u, s])[0],
            "sibling": sorted([u, s])[1]
        })

    def _upsert_uuid_sibling_l(self, usl):
        self.executemany(self._upsert_uuid_sibling_query, [
            {
                "uuid": sorted(x)[0],
                "sibling": sorted(x)[1]
            } for x in usl
        ])

private_buckets = {
    "debugfile"
}

class MediaObject(object):
    """Helper that represents media objects from the db.

    This object represents the join between the media, media_objects,
    and objects tables in the DB though not every instance of it will
    have all that data.

    the association of media to objects is many to many, when fetching
    with this object by url or etag it will attempt to find the most
    recently associated record on the other side of the join.

    """

    __slots__ = (
        'url', 'type', 'owner', 'mime', 'last_check', 'last_status',
        'modified', 'etag', 'detected_mime', 'derivatives', 'bucket'
    )

    def __init__(self, *args, **kwargs):
        for s, a in itertools.izip_longest(self.__slots__, args):
            setattr(self, s, a)
        for kw in kwargs.items():
            setattr(self, *kw)

    def __repr__(self):
        values = {k: getattr(self, k) for k in self.__slots__}
        s = ", ".join("{0}={1!r}".format(k, v)
                      for k, v in values.items() if v is not None)
        return "{0}({1})".format(self.__class__.__name__, s)

    @staticmethod
    def oldmediaapietag(url):
        """Extract the etag from an old media api url

        E.g. http://media.idigbio.org/lookup/images/6d8fbdfc351a5e085aabdf081fb2c5e6?size=fullsize
        """
        re_etag = '^https?://media.idigbio.org/lookup/images/([^?]+)'
        m = re.search(re_etag, url)
        return m and m.group(1)


    @classmethod
    def fromurl(cls, url, idbmodel=apidbpool):
        # Patch special case from old media url.
        etag = cls.oldmediaapietag(url)
        if etag:
            return cls.frometag(etag, idbmodel=idbmodel)

        sql = """
            SELECT DISTINCT ON(media.url)
                   media.url, media.type, media.owner, media.mime,
                   media.last_check, media.last_status, media_objects.modified,
                   objects.etag, objects.detected_mime, objects.derivatives,
                   objects.bucket
                 FROM media
                 LEFT JOIN media_objects ON media.url = media_objects.url
                 LEFT JOIN objects ON media_objects.etag = objects.etag
                 WHERE media.url = %s
                 ORDER BY media.url, media_objects.modified DESC
        """
        r = idbmodel.fetchone(sql, (url,), cursor_factory=cursor)
        if r:
            return cls(*r)

    @classmethod
    def frometag(cls, etag, idbmodel=apidbpool):
        sql = """
            SELECT DISTINCT ON(objects.etag)
                   media.url, media.type, media.owner, media.mime,
                   media.last_check, media.last_status, media_objects.modified,
                   objects.etag, objects.detected_mime, objects.derivatives,
                   objects.bucket
                 FROM objects
                 LEFT JOIN media_objects ON media_objects.etag = objects.etag
                 LEFT JOIN media ON media.url = media_objects.url
                 WHERE objects.etag = %s
                 ORDER BY objects.etag, media_objects.modified DESC
        """
        r = idbmodel.fetchone(sql, (etag,), cursor_factory=cursor)
        if r:
            return cls(*r)

    @classmethod
    def query(cls, conditions=None, params=tuple([]), limit=100, idbmodel=apidbpool):
        "A more general query, the conditions need to be only against media table."
        if conditions:
            where = "WHERE " + " AND ".join(conditions)
        else:
            where = ""

        sql = """
            SELECT DISTINCT ON(media.url)
                   media.url, media.type, media.owner, media.mime,
                   media.last_check, media.last_status, media_objects.modified,
                   objects.etag, objects.detected_mime, objects.derivatives,
                   objects.bucket
                 FROM (SELECT * FROM media {0} LIMIT {1}) AS media
                 JOIN media_objects ON media.url = media_objects.url
                 JOIN objects ON media_objects.etag = objects.etag
                 ORDER BY media.url, media_objects.modified DESC
        """.format(where, limit)
        results = idbmodel.fetchall(sql, params, cursor_factory=cursor)
        return [cls(*r) for r in results]

    @classmethod
    def fromuuid(cls, uuid, idbmodel=None):
        uuid = str(uuid)
        rec = idbmodel.get_item(uuid)
        if rec is not None:
            ref = get_accessuri(rec["type"], rec["data"])["accessuri"]
            return cls.fromurl(ref, idbmodel=idbmodel)

    @classmethod
    def frombuff(cls, buff, **attrs):
        return cls.fromobj(StringIO(buff), **attrs)

    @classmethod
    def fromobj(cls, obj, **attrs):
        obj.seek(0)
        attrs.setdefault('last_status', 200)
        attrs.setdefault('last_check', datetime.now())
        attrs.setdefault('derivatives', False)

        mo = cls(**attrs)
        if not mo.detected_mime or not mo.bucket:
            mo.detected_mime, mo.bucket = validate(
                obj.read(1024), url=mo.url,
                type=mo.type or mo.bucket,
                mime=mo.mime or mo.detected_mime)

        if mo.type and not mo.bucket:
            mo.bucket = mo.type
        if mo.bucket and not mo.type:
            mo.type = mo.bucket
        if not mo.mime:
            mo.mime = mo.detected_mime

        obj.seek(0)
        mo.etag = calcFileHash(obj, op=False, return_size=False)
        if attrs.get('etag'):
            if mo.etag != attrs.get('etag'):
                raise EtagMismatchError(attrs.get('etag'), mo.etag)

        obj.seek(0)
        return mo

    @property
    def keyname(self):
        return self.etag

    @property
    def bucketname(self):
        return "idigbio-{0}-{1}".format(self.bucket, config.ENV)

    def get_key(self, media_store):
        return media_store.get_key(self.keyname, self.bucketname)

    def upload(self, media_store, fobj, force=False):
        k = self.get_key(media_store)
        if force or not k.exists():
            try:
                fobj.seek(0)
                k.set_contents_from_file(fobj, md5=k.get_md5_from_hexdigest(self.etag))
            except AttributeError:
                k.set_contents_from_string(fobj, md5=k.get_md5_from_hexdigest(self.etag))
            if self.bucket not in private_buckets:
                k.make_public()
        return k

    def ensure_object(self, idbmodel):
        return idbmodel.execute(
            """INSERT INTO objects (bucket, etag, detected_mime, derivatives)
               SELECT %s, %s, %s, false WHERE NOT EXISTS (SELECT 1 FROM objects WHERE etag=%s)""",
            (self.bucket, self.etag, self.detected_mime, self.etag))

    def insert_object(self, idbmodel):
        return self.ensure_object(idbmodel)

    def update_media(self, idbmodel):
        return idbmodel.execute(
            """UPDATE media
               SET last_check=now(), last_status=%s, type=%s, mime=%s
               WHERE url=%s""",
            (self.last_status, self.type, self.mime, self.url))

    def insert_media(self, idbmodel):
        return idbmodel.execute(
            """INSERT INTO media (url, type, mime, last_status, last_check, owner)
               VALUES (%s, %s, %s, %s, %s, %s)""",
            (self.url, self.type, self.mime, self.last_status, self.last_check,
             self.owner))

    def ensure_media(self, idbmodel):
        rc = self.update_media(idbmodel)
        if rc == 0:
            return self.insert_media(idbmodel)
        else:
            return rc

    def ensure_media_object(self, idbmodel):
        return idbmodel.execute(
            """INSERT INTO media_objects (url, etag)
               SELECT %(url)s, %(etag)s WHERE NOT EXISTS (
                 SELECT 1 FROM media_objects WHERE url=%(url)s AND etag=%(etag)s
               )""",
            {"url": self.url, "etag": self.etag})

    @staticmethod
    def create_schema(idbmodel=None):
        "Create the schema for media and objects"
        if idbmodel is None:
            idbmodel = PostgresDB()
        with idbmodel:
            idbmodel.execute("BEGIN")
            idbmodel.execute("""CREATE TABLE IF NOT EXISTS media (
                id BIGSERIAL PRIMARY KEY,
                url text UNIQUE,
                type varchar(20),
                mime varchar(255),
                last_status integer,
                last_check timestamp
            )
            """)
            idbmodel.execute("""CREATE TABLE IF NOT EXISTS objects (
                id BIGSERIAL PRIMARY KEY,
                bucket varchar(255) NOT NULL,
                etag varchar(41) NOT NULL UNIQUE,
                detected_mime varchar(255),
                derivatives boolean DEFAULT false
            )
            """)
            idbmodel.execute("""CREATE TABLE IF NOT EXISTS media_objects (
                id BIGSERIAL PRIMARY KEY,
                url text NOT NULL REFERENCES media(url),
                etag varchar(41) NOT NULL REFERENCES objects(etag),
                modified timestamp NOT NULL DEFAULT now()
            )
            """)
            idbmodel.commit()


class RecordSet(object):
    __slots__ = [
        "id", "uuid", "publisher_uuid", "name", "recordids",
        "eml_link", "file_link", "ingest", "first_seen",
        "last_seen", "pub_date", "file_harvest_date",
        "file_harvest_etag","eml_harvest_date", "eml_harvest_etag"
    ]

    def __init__(self, **attrs):
        for i in self.__slots__:
            setattr(self, i, None)
        for i in attrs.items():
            setattr(self, *i)

    @classmethod
    def fromuuid(cls, uuid, idbmodel=apidbpool):
        sql = """
        SELECT id, uuid, publisher_uuid, name, recordids, eml_link, file_link,
            ingest, first_seen, last_seen, pub_date, file_harvest_date, file_harvest_etag,
            eml_harvest_date, eml_harvest_etag
        FROM recordsets
        WHERE uuid = %s"""
        r = idbmodel.fetchone(sql, (uuid, ), cursor_factory=DictCursor)
        if r:
            return RecordSet(**r)

    @staticmethod
    def fetch_file(uuid, filename, idbmodel=apidbpool, media_store=None, logger=logger):
        sql = """
            SELECT uuid, etag, objects.bucket
            FROM recordsets
            LEFT JOIN objects on recordsets.file_harvest_etag = objects.etag
            WHERE recordsets.uuid = %s
        """
        r = idbmodel.fetchone(sql, (uuid,), cursor_factory=cursor)
        if not r:
            raise ValueError("No recordset with uuid {0!r}".format(uuid))
        uuid, etag, bucket = r
        if not etag:
            raise ValueError("Recordset {0!r} doesn't have a stored object".format(uuid))
        logger.info("Fetching etag %s to %r", etag, filename)
        bucketname = "idigbio-{0}-{1}".format(bucket, os.environ["ENV"])
        k = media_store.get_key(etag, bucketname)
        media_store.get_contents_to_filename(k, filename, md5=etag)
