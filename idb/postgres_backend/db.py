import contextlib
import uuid
import datetime
import random
import json
import hashlib
import sys

from psycopg2.extras import DictCursor
from psycopg2.extensions import (ISOLATION_LEVEL_READ_COMMITTED,
                                 ISOLATION_LEVEL_AUTOCOMMIT,
                                 TRANSACTION_STATUS_IDLE)
from idb.helpers.etags import calcEtag
from idb.postgres_backend import apidbpool

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
        ) AS latest
        ON latest.uuids_id=uuids.id
    """

    __join_uuids_etags_all_versions = """
            LEFT JOIN uuids_data as latest
            ON latest.uuids_id=uuids.id
    """

    __join_uuids_identifiers = """
        LEFT JOIN LATERAL (
            SELECT uuids_id, array_agg(identifier) as recordids
            FROM uuids_identifier
            WHERE uuids_id=uuids.id
            GROUP BY uuids_id
        ) as ids
        ON ids.uuids_id=uuids.id
    """

    __join_uuids_siblings = """
            LEFT JOIN LATERAL (
            SELECT subject, json_object_agg(rel,array_agg) as siblings
            FROM (
                SELECT subject, rel, array_agg(object)
                FROM (
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
                    ) as rel_union
                    JOIN uuids
                    ON r2=id
                    WHERE uuids.deleted = false
                ) as rel_table
                WHERE subject=uuids.id
                GROUP BY subject, rel
            ) as rels
            GROUP BY subject
        ) as sibs
        ON sibs.subject=uuids.id
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
            data,
            riak_etag
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

    def __init__(self, pool=None):

        self._pool = (pool or apidbpool)
        # Generic reusable cursor for normal ops
        self.conn = self._pool.get()

    def __del__(self):
        if self.conn:
            self._pool.put(self.conn)

    def __enter__(self):
        pass

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
            return cur.execute(*args)

    def executemany(self, *args, **kwargs):
        with self.cursor(**kwargs) as cur:
            return cur.executemany(*args)

    def commit(self):
        self.conn.commit()

    def rollback(self):
        self.conn.rollback()

    def cursor(self, ss=False, **kwargs):
        if kwargs.pop('named', False) is True:
            kwargs['name'] == str(uuid.uuid4())

        kwargs.setdefault('cursor_factory', DictCursor)
        return self.conn.cursor(**kwargs)

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
            riak_etag varchar(41),
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
        self._upsert_uuid_data(u, tombstone_etag)
        self.execute("UPDATE uuids SET deleted=true WHERE id=%s", (u,))

    def undelete_item(self, u):
        # Needs to be accompanied by a corresponding version insertion to obsolete the tombstone
        self.execute("UPDATE uuids SET deleted=false WHERE id=%s", (u,))

    @classmethod
    def get_type_list(self, t, limit=100, offset=0, data=False):
        if data:
            if limit is not None:
                sql = ("SELECT * FROM (" + self.__item_master_query_data + """
                    WHERE deleted=false and type=%s
                    LIMIT %s OFFSET %s
                """ + ") AS a ORDER BY uuid", (t, limit, offset))
            else:
                sql = ("SELECT * FROM (" + self.__item_master_query_data + """
                    WHERE deleted=false and type=%s
                """ + ") AS a ORDER BY uuid", (t,))
        else:
            if limit is not None:
                sql = ("SELECT * FROM (" + self.__item_master_query + """
                    WHERE deleted=false and type=%s
                    LIMIT %s OFFSET %s
                """ + ") AS a ORDER BY uuid", (t, limit, offset))
            else:
                sql = ("SELECT * FROM (" + self.__item_master_query + """
                    WHERE deleted=false and type=%s
                """ + ") AS a ORDER BY uuid", (t,))
        return apidbpool.fetchiter(*sql)

    @staticmethod
    def get_type_count(t):
        sql = ("""SELECT count(*) as count
                  FROM uuids
                  WHERE deleted=false and type=%s""", (t,))
        return apidbpool.fetchone(*sql)[0]

    @classmethod
    def get_children_list(self, u, t, limit=100, offset=0, data=False):
        sql = None
        if data:
            if limit is not None:
                sql = ("SELECT * FROM (" + self.__item_master_query_data + """
                    WHERE deleted=false and type=%s and parent=%s
                    LIMIT %s OFFSET %s
                """ + ") AS a ORDER BY uuid", (t, u, limit, offset))
            else:
                sql = ("SELECT * FROM (" + self.__item_master_query_data + """
                    WHERE deleted=false and type=%s and parent=%s
                """ + ") AS a ORDER BY uuid", (t, u))
        else:
            if limit is not None:
                sql = ("SELECT * FROM (" + self.__item_master_query + """
                    WHERE deleted=false and type=%s and parent=%s
                    LIMIT %s OFFSET %s
                """ + ") AS a ORDER BY uuid", (t, u, limit, offset))
            else:
                sql = ("SELECT * FROM (" + self.__item_master_query + """
                    WHERE deleted=false and type=%s and parent=%s
                """ + ") AS a ORDER BY uuid", (t, u))
        return apidbpool.fetchiter(*sql, named=True)

    @classmethod
    def get_children_count(self, u, t):
        sql = (""" SELECT
            count(*) as count FROM uuids
            WHERE deleted=false and type=%s and parent=%s
        """, (t, u))
        return apidbpool.fetchone(*sql)[0]

    def _id_precheck(self, u, ids):
        rows = self.fetchall("""SELECT
            identifier,
            uuids_id
            FROM uuids_identifier
            WHERE uuids_id=%s OR identifier = ANY(%s)
        """, (u, ids))
        for row in rows:
            if row["uuids_id"] != u:
                return False
        else:
            return True

    def get_uuid(self, ids):
        sql = ("""SELECT
            identifier,
            uuids_id,
            parent,
            deleted
            FROM uuids_identifier
            JOIN uuids
            ON uuids.id = uuids_identifier.uuids_id
            WHERE identifier = ANY(%s)
        """, (ids,))
        rid = None
        parent = None
        deleted = False
        for row in self.fetchall(*sql):
            if rid is None:
                rid = row["uuids_id"]
                parent = row["parent"]
                deleted = row["deleted"]
            elif rid == row["uuids_id"]:
                pass
            else:
                return (None, parent, deleted)
        if rid is None:
            rv = (str(uuid.uuid4()), parent, deleted)
            #print "Create UUID", ids, rv
            return rv
        else:
            return (rid, parent, deleted)

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
            print u, t, ids
            raise
        except:
            print u, t, ids
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
            raise e[1], None, e[2]

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
        self.execute(self._upsert_uuid_data_query, {
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


def main():
    db = PostgresDB()
    print db.delete_item("00000000-0000-0000-0000-000000000000")
    # if os.environ["ENV"] == "test":
    #     import requests
    #     ses = requests.Session()

    #     print("Creating test schema")
    #     db = PostgresDB()
    #     db.drop_schema()
    #     db.create_schema()

    #     r = ses.get("http://api.idigbio.org/v1/records/")
    #     r.raise_for_status()
    #     ro = r.json()

    #     reccount = 0
    #     mediarecords = set()
    #     for rec in ro["idigbio:items"]:
    #         print "record", rec["idigbio:uuid"]
    #         rr = ses.get(
    #             "http://api.idigbio.org/v1/records/{0}".format(rec["idigbio:uuid"]))
    #         rr.raise_for_status()
    #         rro = rr.json()
    #         mrs = []
    #         if "mediarecord" in rro["idigbio:links"]:
    #             mrs = [s.split("/")[-1]
    #                    for s in rro["idigbio:links"]["mediarecord"]]
    #         mediarecords.update(mrs)
    #         db.set_record(
    #             rro["idigbio:uuid"],
    #             "record",
    #             rro["idigbio:links"]["recordset"][0].split("/")[-1],
    #             rro["idigbio:data"],
    #             rro["idigbio:recordIds"],
    #             []
    #         )
    #         reccount += 1

    #     for mrid in mediarecords:
    #         print "mediarecord", mrid
    #         rr = ses.get(
    #             "http://api.idigbio.org/v1/mediarecords/{0}".format(mrid))
    #         rr.raise_for_status()
    #         rro = rr.json()
    #         recs = [s.split("/")[-1] for s in rro["idigbio:links"]["record"]]
    #         mediarecords.update(mrs)
    #         db.set_record(
    #             rro["idigbio:uuid"],
    #             "mediarecord",
    #             rro["idigbio:links"]["recordset"][0].split("/")[-1],
    #             rro["idigbio:data"],
    #             rro["idigbio:recordIds"],
    #             recs
    #         )

    #     db.commit()
    #     print "Imported ", reccount, "records and ", len(mediarecords), "mediarecords."
    # else:
    #     print "ENV not test, refusing to run"

if __name__ == '__main__':
    main()
