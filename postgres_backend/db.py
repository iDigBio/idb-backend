import psycopg2
import uuid
import datetime
import random
import json
import hashlib
import sys
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

TEST_SIZE=10000
TEST_COUNT=10

from . import *

from helpers.etags import calcEtag

class PostgresDB:
    __item_master_query_from = """FROM uuids
        LEFT JOIN LATERAL (
            SELECT * FROM uuids_data
            WHERE uuids_id=uuids.id
            ORDER BY modified DESC
            LIMIT 1
        ) AS latest
        ON latest.uuids_id=uuids.id
        LEFT JOIN LATERAL (
            SELECT uuids_id, array_agg(identifier) as recordids
            FROM uuids_identifier
            WHERE uuids_id=uuids.id
            GROUP BY id
        ) as ids
        ON ids.uuids_id=uuids.id
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

    __item_master_query = """ SELECT
            uuids.id as uuid,
            type,
            deleted,
            data_etag as etag,
            version,
            modified,
            parent,
            recordids,
            siblings
    """ + __item_master_query_from

    __item_master_query_data = """ SELECT
            uuids.id as uuid,
            type,
            deleted,
            data_etag as etag,
            version,
            modified,
            parent,
            recordids,
            siblings,
            data,
            riak_etag
    """ + __item_master_query_from + """
        LEFT JOIN data
        ON data_etag = etag
    """

    __upsert_uuid_query = """INSERT INTO uuids (id,type,parent)
        SELECT %(uuid)s, %(type)s, %(parent)s WHERE NOT EXISTS (
            SELECT 1 FROM uuids WHERE id=%(uuid)s
        )
    """

    __upsert_data_query = """INSERT INTO data (etag,data)
        SELECT %(etag)s, %(data)s WHERE NOT EXISTS (
            SELECT 1 FROM data WHERE etag=%(etag)s
        )
    """

    __upsert_uuid_data_query = """WITH v AS (
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

    __upsert_uuid_id_query = """INSERT INTO uuids_identifier (uuids_id, identifier)
        SELECT %(uuid)s, %(id)s WHERE NOT EXISTS (
            SELECT 1 FROM uuids_identifier WHERE identifier=%(id)s
        )
    """

    __upsert_uuid_sibling_query = """INSERT INTO uuids_siblings (r1,r2)
        SELECT %(uuid)s, %(sibling)s WHERE NOT EXISTS (
            SELECT 1 FROM uuids_siblings WHERE (r1=%(uuid)s and r2=%(sibling)s) or (r2=%(uuid)s and r1=%(sibling)s)
        )
    """

    def __init__(self):

        # Generic reusable cursor for normal ops
        self._cur = pg.cursor(cursor_factory=DictCursor)

    def commit(self):
        pg.commit()

    def rollback(self):
        pg.rollback()

    def drop_schema(self,commit=True):
        self._cur.execute("DROP VIEW IF EXISTS idigbio_uuids_new")
        self._cur.execute("DROP VIEW IF EXISTS idigbio_uuids_data")
        self._cur.execute("DROP VIEW IF EXISTS idigbio_relations")
        self._cur.execute("DROP TABLE IF EXISTS uuids_siblings")
        self._cur.execute("DROP TABLE IF EXISTS uuids_identifier")
        self._cur.execute("DROP TABLE IF EXISTS uuids_data")
        self._cur.execute("DROP TABLE IF EXISTS uuids")
        self._cur.execute("DROP TABLE IF EXISTS data")

        if commit:
            self.commit()

    def create_schema(self,commit=True):

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

        self._cur.execute("CREATE INDEX uuids_data_uuids_id ON uuids_data (uuids_id)")
        self._cur.execute("CREATE INDEX uuids_data_modified ON uuids_data (modified)")
        self._cur.execute("CREATE INDEX uuids_data_version ON uuids_data (version)")
        self._cur.execute("CREATE INDEX uuids_deleted ON uuids (deleted)")
        self._cur.execute("CREATE INDEX uuids_parent ON uuids (parent)")
        self._cur.execute("CREATE INDEX uuids_type ON uuids (type)")
        self._cur.execute("CREATE INDEX uuids_siblings_r1 ON uuids_siblings (r1)")
        self._cur.execute("CREATE INDEX uuids_siblings_r2 ON uuids_siblings (r2)")
        self._cur.execute("CREATE INDEX uuids_identifier_uuids_id ON uuids_identifier (uuids_id)")
        self._cur.execute("CREATE OR REPLACE VIEW idigbio_uuids_new AS" + self.__item_master_query)
        self._cur.execute("CREATE OR REPLACE VIEW idigbio_uuids_data AS" + self.__item_master_query_data)

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

    def __get_ss_cursor(self,name=None):
        """ Get a named server side cursor for large ops"""

        if name is None:
            return pg.cursor(str(uuid.uuid4()),cursor_factory=DictCursor)
        else:
            return pg.cursor(name,cursor_factory=DictCursor)

    def get_item(self,u,version=None):
        if version is not None:
            # Fetch by version ignores the deleted flag
            self._cur.execute(self.__item_master_query_data + """
                WHERE uuids.id=%s and version=%s
            """, (u,version))
        else:
            self._cur.execute(self.__item_master_query_data + """
                WHERE deleted=false and uuids.id=%s
            """, (u,))
        return self._cur.fetchone()

    def get_type_list(self, t, limit=100, offset=0):
        cur = self.__get_ss_cursor()
        if limit is not None:
            cur.execute(self.__item_master_query + """
                WHERE deleted=false and type=%s
                ORDER BY uuid
                LIMIT %s OFFSET %s
            """, (t,limit,offset))
        else:
            cur.execute(self.__item_master_query + """
                WHERE deleted=false and type=%s
                ORDER BY uuid
            """, (t,))
        for r in cur:
            yield r

    def get_type_count(self, t):
        cur = self.__get_ss_cursor()
        cur.execute(""" SELECT
            count(*) as count FROM uuids
            WHERE deleted=false and type=%s
        """, (t,))
        return cur.fetchone()["count"]


    def get_children_list(self, u, t, limit=100, offset=0):
        cur = self.__get_ss_cursor()
        if limit is not None:
            cur.execute(self.__item_master_query + """
                WHERE deleted=false and type=%s and parent=%s
                ORDER BY uuid
                LIMIT %s OFFSET %s
            """,(t,u,limit,offset))
        else:
            cur.execute(self.__item_master_query + """
                WHERE deleted=false and type=%s and parent=%s
                ORDER BY uuid
            """,(t,u))
        for r in cur:
            yield r

    def get_children_count(self, u, t):
        cur = self.__get_ss_cursor()
        cur.execute(""" SELECT
            count(*) as count FROM uuids
            WHERE deleted=false and type=%s and parent=%s
        """, (t,u))
        return cur.fetchone()["count"]

    def _id_precheck(self, u, ids):
        self._cur.execute("""SELECT
            identifier,
            uuids_id
            FROM uuids_identifier
            WHERE uuids_id=%s OR identifier = ANY(%s)
        """, (u,ids))
        consistent = False
        for row in self._cur:
            if row["uuids_id"] != u:
                break
        else:
            consistent = True
        return consistent

    def get_uuid(self, ids):
        self._cur.execute("""SELECT
            identifier,
            uuids_id
            FROM uuids_identifier
            WHERE identifier = ANY(%s)
        """, (ids,))
        rid = None
        for row in self._cur:
            if rid is None:
                rid = r["uuids_id"]
            elif rid == r["uuids_id"]:
                pass
            else:
                return None
        if rid is None:
            return uuid.uuid4()
        else:
            return rid

    def set_record(self, u, t, p, d, ids, siblings, commit=True):
        try:
            assert self._id_precheck(u, ids)
            e = calcEtag(d)
            self.__upsert_uuid(u, t, p, commit=False)
            self.__upsert_data(e, d, commit=False)
            self.__upsert_uuid_data(u, e, commit=False)
            self.__upsert_uuid_id_l([(u,i) for i in ids], commit=False)
            self.__upsert_uuid_sibling_l([(u,s) for s in siblings], commit=False)
            if commit:
                self.commit()
        except:
            e = sys.exc_info()
            self.rollback()
            raise e[1], None, e[2]


    def set_records(self, record_list, commit=True):
        try:
            for u, t, p, d, ids, siblings in record_list:
                assert self._id_precheck(u, ids)
                e = calcEtag(d)
                self.__upsert_uuid(u, t, p, commit=False)
                self.__upsert_data(e, d, commit=False)
                self.__upsert_uuid_data(u, e, commit=False)
                self.__upsert_uuid_id_l([(u,i) for i in ids], commit=False)
                self.__upsert_uuid_sibling_l([(u,s) for s in siblings], commit=False)
            if commit:
                self.commit()
        except:
            e = sys.exc_info()
            self.rollback()
            raise e[1], None, e[2]

    # UUID

    def __upsert_uuid(self, u, t, p, commit=True):
        self._cur.execute(self.__upsert_uuid_query, {
            "uuid": u,
            "type": t,
            "parent": p
        })
        if commit:
            self.commit()

    def __upsert_uuid_l(self, utpl, commit=True):
        self._cur.executemany(self.__upsert_uuid_query, [
            {
            "uuid": u,
            "type": t,
            "parent": p
            } for u, t, p in utpl
        ])
        if commit:
            self.commit()

    # DATA
    def __upsert_data(self, e, d, commit=True):
        self._cur.execute(self.__upsert_data_query, {
            "etag": e,
            "data": json.dumps(d)
        })
        if commit:
            self.commit()

    def __upsert_data_l(self, edl, commit=True):
        self._cur.executemany(self.__upsert_data_query, [
            {
            "etag": e,
            "data": json.dumps(d)
            } for e, d in edl
        ])
        if commit:
            self.commit()

    # ETAGS ONLY
    def _upsert_etag_l(self, el,commit=True):
        self._cur.executemany("""INSERT INTO data (etag)
            SELECT %(etag)s as etag WHERE NOT EXISTS (
                SELECT 1 FROM data WHERE etag=%(etag)s
            )
        """, [{"etag": e } for e in el])
        if commit:
            self.commit()

    # UUID DATA
    def __upsert_uuid_data(self, u, e, commit=True):
        self._cur.execute(self.__upsert_uuid_data_query, {
            "uuid": u,
            "etag": e
        })
        if commit:
            self.commit()

    def __upsert_uuid_data_l(self, uel, commit=True):
        self._cur.executemany(self.__upsert_uuid_data_query, [
            {
                "uuid": u,
                "etag": e
            } for u, e in uel
        ])
        if commit:
            self.commit()

    # UUID ID
    def __upsert_uuid_id(self, u, i, commit=True):
        self._cur.execute(self.__upsert_uuid_id_query, {
            "uuid": u,
            "id": i
        })
        if commit:
            self.commit()

    def __upsert_uuid_id_l(self, uil, commit=True):
        self._cur.executemany(self.__upsert_uuid_id_query, [
            {
                "uuid": u,
                "id": i
            } for u, i in uil
        ])
        if commit:
            self.commit()

    # UUID ID
    def __upsert_uuid_sibling(self, u, s, commit=True):
        self._cur.execute(self.__upsert_uuid_sibling_query, {
            "uuid": u,
            "sibling": s
        })
        if commit:
            self.commit()

    def __upsert_uuid_sibling_l(self, usl, commit=True):
        self._cur.executemany(self.__upsert_uuid_sibling_query, [
            {
                "uuid": u,
                "sibling": s
            } for u, s in usl
        ])
        if commit:
            self.commit()

def main():
    import requests
    ses = requests.Session()

    print("Creating test schema")
    db = PostgresDB()
    db.drop_schema()
    db.create_schema()

    r = ses.get("http://api.idigbio.org/v1/records/")
    r.raise_for_status()
    ro = r.json()

    reccount = 0
    mediarecords = set()
    for rec in ro["idigbio:items"]:
        print "record", rec["idigbio:uuid"]
        rr = ses.get("http://api.idigbio.org/v1/records/{0}".format(rec["idigbio:uuid"]))
        rr.raise_for_status()
        rro = rr.json()
        mrs = []
        if "mediarecord" in rro["idigbio:links"]:
            mrs = [s.split("/")[-1] for s in rro["idigbio:links"]["mediarecord"]]
        mediarecords.update(mrs)
        db.set_record(
            rro["idigbio:uuid"],
            "record",
            rro["idigbio:links"]["recordset"][0].split("/")[-1],
            rro["idigbio:data"],
            rro["idigbio:recordIds"],
            [],
            commit=False
        )
        reccount += 1

    for mrid in mediarecords:
        print "mediarecord", mrid
        rr = ses.get("http://api.idigbio.org/v1/mediarecords/{0}".format(mrid))
        rr.raise_for_status()
        rro = rr.json()
        recs = [s.split("/")[-1] for s in rro["idigbio:links"]["record"]]
        mediarecords.update(mrs)
        db.set_record(
            rro["idigbio:uuid"],
            "mediarecord",
            rro["idigbio:links"]["recordset"][0].split("/")[-1],
            rro["idigbio:data"],
            rro["idigbio:recordIds"],
            recs,
            commit=False
        )

    db.commit()
    print "Imported ", reccount, "records and ", len(mediarecords), "mediarecords."


if __name__ == '__main__':
    main()