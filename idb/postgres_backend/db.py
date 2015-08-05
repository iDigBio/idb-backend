import psycopg2
import uuid
import datetime
import random
import json
import hashlib
import sys

TEST_SIZE = 10000
TEST_COUNT = 10

#tombstone_etag = calcEtag({"deleted":True})
tombstone_etag = "9a4e35834eb80d9af64bcd07ed996b9ec0e60d92"

from . import *

from idb.helpers.etags import calcEtag


class PostgresDB:
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

    def __init__(self):

        # Generic reusable cursor for normal ops
        self._cur = pg.cursor(cursor_factory=DictCursor)
        self._pg = pg

    def commit(self):
        pg.commit()

    def rollback(self):
        pg.rollback()

    def cursor(self, ss=False, ss_name=None):
        if ss:
            return self._get_ss_cursor(name=ss_name)
        else:
            return self._cur

    def drop_schema(self, commit=True):
        raise Exception("I can't let you do that, dave.")
        # self._cur.execute("DROP VIEW IF EXISTS idigbio_uuids_new")
        # self._cur.execute("DROP VIEW IF EXISTS idigbio_uuids_data")
        # self._cur.execute("DROP VIEW IF EXISTS idigbio_relations")
        # self._cur.execute("DROP TABLE IF EXISTS uuids_siblings")
        # self._cur.execute("DROP TABLE IF EXISTS uuids_identifier")
        # self._cur.execute("DROP TABLE IF EXISTS uuids_data")
        # self._cur.execute("DROP TABLE IF EXISTS uuids")
        # self._cur.execute("DROP TABLE IF EXISTS data")

        # if commit:
        #     self.commit()

    def create_views(self, commit=True):
        self._cur.execute(
            "CREATE OR REPLACE VIEW idigbio_uuids_new AS" + self.__item_master_query)
        self._cur.execute(
            "CREATE OR REPLACE VIEW idigbio_uuids_data AS" + self.__item_master_query_data)

        self._cur.execute("""CREATE OR REPLACE VIEW idigbio_relations AS
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

        if commit:
            self.commit()

    def create_schema(self, commit=True):

        self._cur.execute("""CREATE TABLE IF NOT EXISTS uuids (
            id uuid NOT NULL PRIMARY KEY,
            type varchar(50) NOT NULL,
            parent uuid,
            deleted boolean NOT NULL DEFAULT false
        )""")

        self._cur.execute("""CREATE TABLE IF NOT EXISTS data (
            etag varchar(41) NOT NULL PRIMARY KEY,
            riak_etag varchar(41),
            data jsonb
        )""")

        self._cur.execute("""CREATE TABLE IF NOT EXISTS uuids_data (
            id bigserial NOT NULL PRIMARY KEY,
            uuids_id uuid NOT NULL REFERENCES uuids(id),
            data_etag varchar(41) NOT NULL REFERENCES data(etag),
            modified timestamp NOT NULL DEFAULT now(),
            version int NOT NULL DEFAULT 0
        )""")

        self._cur.execute("""CREATE TABLE IF NOT EXISTS uuids_identifier (
            id bigserial NOT NULL PRIMARY KEY,
            identifier text NOT NULL UNIQUE,
            uuids_id uuid NOT NULL REFERENCES uuids(id)
        )""")

        self._cur.execute("""CREATE TABLE IF NOT EXISTS uuids_siblings (
            id bigserial NOT NULL PRIMARY KEY,
            r1 uuid NOT NULL REFERENCES uuids(id),
            r2 uuid NOT NULL REFERENCES uuids(id)
        )""")

        self._cur.execute(
            "CREATE INDEX uuids_data_uuids_id ON uuids_data (uuids_id)")
        self._cur.execute(
            "CREATE INDEX uuids_data_modified ON uuids_data (modified)")
        self._cur.execute(
            "CREATE INDEX uuids_data_version ON uuids_data (version)")
        self._cur.execute("CREATE INDEX uuids_deleted ON uuids (deleted)")
        self._cur.execute("CREATE INDEX uuids_parent ON uuids (parent)")
        self._cur.execute("CREATE INDEX uuids_type ON uuids (type)")
        self._cur.execute(
            "CREATE INDEX uuids_siblings_r1 ON uuids_siblings (r1)")
        self._cur.execute(
            "CREATE INDEX uuids_siblings_r2 ON uuids_siblings (r2)")
        self._cur.execute(
            "CREATE INDEX uuids_identifier_uuids_id ON uuids_identifier (uuids_id)")

        self.create_views(commit=False)

        if commit:
            self.commit()

    def _get_ss_cursor(self, name=None):
        """ Get a named server side cursor for large ops"""

        cur = None
        if name is None:
            cur = pg.cursor(str(uuid.uuid4()), cursor_factory=DictCursor)
        else:
            cur = pg.cursor(name, cursor_factory=DictCursor)
        return cur

    def get_item(self, u, version=None):
        if version is not None:
            # Fetch by version ignores the deleted flag
            if version == "all":
                self._cur.execute(self.__columns_master_query_data +
                                  """ FROM uuids """ +
                                  self.__join_uuids_etags_all_versions +
                                  self.__join_uuids_identifiers +
                                  self.__join_uuids_siblings +
                                  self.__join_uuids_data +
                                  """
                    WHERE uuids.id=%s
                    ORDER BY version ASC
                """, (u,))
                return self._cur.fetchall()
            else:
                # Fetch by version ignores the deleted flag
                self._cur.execute(self.__columns_master_query_data +
                                  """ FROM uuids """ +
                                  self.__join_uuids_etags_all_versions +
                                  self.__join_uuids_identifiers +
                                  self.__join_uuids_siblings +
                                  self.__join_uuids_data +
                                  """
                    WHERE uuids.id=%s and version=%s
                """, (u, version))
        else:
            self._cur.execute(self.__item_master_query_data + """
                WHERE deleted=false and uuids.id=%s
            """, (u,))
        rec = self._cur.fetchone()
        self.rollback()
        return rec

    def delete_item(self, u, commit=True):
        self._upsert_uuid_data(u, tombstone_etag, commit=False) 
        self._cur.execute("UPDATE uuids SET deleted=true WHERE id=%s", (u,))
        if commit:
            self.commit()

    def undelete_item(self, u, commit=True):
        # Needs to be accompanied by a corresponding version insertion to obsolete the tombstone
        self._cur.execute("UPDATE uuids SET deleted=false WHERE id=%s", (u,))
        if commit:
            self.commit()

    def get_type_list(self, t, limit=100, offset=0, data=False):
        cur = self._get_ss_cursor()
        if data:
            if limit is not None:
                cur.execute(self.__item_master_query_data + """
                    WHERE deleted=false and type=%s
                    ORDER BY uuid
                    LIMIT %s OFFSET %s
                """, (t, limit, offset))
            else:
                cur.execute(self.__item_master_query_data + """
                    WHERE deleted=false and type=%s
                    ORDER BY uuid
                """, (t,))
        else:
            if limit is not None:
                cur.execute(self.__item_master_query + """
                    WHERE deleted=false and type=%s
                    ORDER BY uuid
                    LIMIT %s OFFSET %s
                """, (t, limit, offset))
            else:
                cur.execute(self.__item_master_query + """
                    WHERE deleted=false and type=%s
                    ORDER BY uuid
                """, (t,))
        for r in cur:
            yield r
        self.rollback()

    def get_type_count(self, t):
        cur = self._get_ss_cursor()
        cur.execute(""" SELECT
            count(*) as count FROM uuids
            WHERE deleted=false and type=%s
        """, (t,))
        count = cur.fetchone()["count"]
        self.rollback()
        return count

    def get_children_list(self, u, t, limit=100, offset=0, data=False):
        cur = self._get_ss_cursor()
        if data:
            if limit is not None:
                cur.execute(self.__item_master_query_data + """
                    WHERE deleted=false and type=%s and parent=%s
                    ORDER BY uuid
                    LIMIT %s OFFSET %s
                """, (t, u, limit, offset))
            else:
                cur.execute(self.__item_master_query_data + """
                    WHERE deleted=false and type=%s and parent=%s
                    ORDER BY uuid
                """, (t, u))
        else:
            if limit is not None:
                cur.execute(self.__item_master_query + """
                    WHERE deleted=false and type=%s and parent=%s
                    ORDER BY uuid
                    LIMIT %s OFFSET %s
                """, (t, u, limit, offset))
            else:
                cur.execute(self.__item_master_query + """
                    WHERE deleted=false and type=%s and parent=%s
                    ORDER BY uuid
                """, (t, u))
        for r in cur:
            yield r
        self.rollback()

    def get_children_count(self, u, t):
        cur = self._get_ss_cursor()
        cur.execute(""" SELECT
            count(*) as count FROM uuids
            WHERE deleted=false and type=%s and parent=%s
        """, (t, u))
        count = cur.fetchone()["count"]
        self.rollback()
        return count

    def _id_precheck(self, u, ids, commit=False):
        self._cur.execute("""SELECT
            identifier,
            uuids_id
            FROM uuids_identifier
            WHERE uuids_id=%s OR identifier = ANY(%s)
        """, (u, ids))
        consistent = False
        for row in self._cur:
            if row["uuids_id"] != u:
                break
        else:
            consistent = True
        if commit:
            self.commit()
        return consistent

    def get_uuid(self, ids, commit=False):
        self._cur.execute("""SELECT
            identifier,
            uuids_id,
            parent,
            deleted
            FROM uuids_identifier
            JOIN uuids
            ON uuids.id = uuids_identifier.uuids_id
            WHERE identifier = ANY(%s)
        """, (ids,))
        if commit:
            self.commit()
        rid = None
        parent = None
        deleted = False
        for row in self._cur:
            if rid is None:
                rid = row["uuids_id"]
                parent = row["parent"]
                deleted = row["deleted"]
            elif rid == row["uuids_id"]:
                pass
            else:
                return (None,parent,deleted)
        if rid is None:
            return (str(uuid.uuid4()),parent,deleted)
        else:
            return (rid,parent,deleted)

    def set_record(self, u, t, p, d, ids, siblings, commit=True):
        try:
            assert self._id_precheck(u, ids, commit=False)
            e = calcEtag(d)
            self._upsert_uuid(u, t, p, commit=False)
            self._upsert_data(e, d, commit=False)
            self._upsert_uuid_data(u, e, commit=False)
            self._upsert_uuid_id_l([(u, i) for i in ids], commit=False)
            self._upsert_uuid_sibling_l(
                [(u, s) for s in siblings], commit=False)
            if commit:
                self.commit()
        except:
            e = sys.exc_info()
            self.rollback()
            raise e[1], None, e[2]

    def set_records(self, record_list, commit=True):
        try:
            for u, t, p, d, ids, siblings in record_list:
                assert self._id_precheck(u, ids, commit=False)
                e = calcEtag(d)
                self._upsert_uuid(u, t, p, commit=False)
                self._upsert_data(e, d, commit=False)
                self._upsert_uuid_data(u, e, commit=False)
                self._upsert_uuid_id_l([(u, i) for i in ids], commit=False)
                self._upsert_uuid_sibling_l(
                    [(u, s) for s in siblings], commit=False)
            if commit:
                self.commit()
        except:
            e = sys.exc_info()
            self.rollback()
            raise e[1], None, e[2]

    # UUID

    def _upsert_uuid(self, u, t, p, commit=True):
        self._cur.execute(self._upsert_uuid_query, {
            "uuid": u,
            "type": t,
            "parent": p
        })
        if commit:
            self.commit()

    def _upsert_uuid_l(self, utpl, commit=True):
        self._cur.executemany(self._upsert_uuid_query, [
            {
                "uuid": u,
                "type": t,
                "parent": p
            } for u, t, p in utpl
        ])
        if commit:
            self.commit()

    # DATA
    def _upsert_data(self, e, d, commit=True):
        self._cur.execute(self._upsert_data_query, {
            "etag": e,
            "data": json.dumps(d)
        })
        if commit:
            self.commit()

    def _upsert_data_l(self, edl, commit=True):
        self._cur.executemany(self._upsert_data_query, [
            {
                "etag": e,
                "data": json.dumps(d)
            } for e, d in edl
        ])
        if commit:
            self.commit()

    # ETAGS ONLY
    def _upsert_etag_l(self, el, commit=True):
        self._cur.executemany("""INSERT INTO data (etag)
            SELECT %(etag)s as etag WHERE NOT EXISTS (
                SELECT 1 FROM data WHERE etag=%(etag)s
            )
        """, [{"etag": e } for e in el])
        if commit:
            self.commit()

    # UUID DATA
    def _upsert_uuid_data(self, u, e, commit=True):
        self._cur.execute(self._upsert_uuid_data_query, {
            "uuid": u,
            "etag": e
        })
        if commit:
            self.commit()

    def _upsert_uuid_data_l(self, uel, commit=True):
        self._cur.executemany(self._upsert_uuid_data_query, [
            {
                "uuid": u,
                "etag": e
            } for u, e in uel
        ])
        if commit:
            self.commit()

    # UUID ID
    def _upsert_uuid_id(self, u, i, commit=True):
        self._cur.execute(self._upsert_uuid_id_query, {
            "uuid": u,
            "id": i
        })
        if commit:
            self.commit()

    def _upsert_uuid_id_l(self, uil, commit=True):
        self._cur.executemany(self._upsert_uuid_id_query, [
            {
                "uuid": u,
                "id": i
            } for u, i in uil
        ])
        if commit:
            self.commit()

    # UUID ID
    def _upsert_uuid_sibling(self, u, s, commit=True):
        self._cur.execute(self._upsert_uuid_sibling_query, {
            "uuid": sorted([u, s])[0],
            "sibling": sorted([u, s])[1]
        })
        if commit:
            self.commit()

    def _upsert_uuid_sibling_l(self, usl, commit=True):
        self._cur.executemany(self._upsert_uuid_sibling_query, [
            {
                "uuid": sorted(x)[0],
                "sibling": sorted(x)[1]
            } for x in usl
        ])
        if commit:
            self.commit()


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
    #             [],
    #             commit=False
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
    #             recs,
    #             commit=False
    #         )

    #     db.commit()
    #     print "Imported ", reccount, "records and ", len(mediarecords), "mediarecords."
    # else:
    #     print "ENV not test, refusing to run"

if __name__ == '__main__':
    main()
