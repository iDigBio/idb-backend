import psycopg2
import uuid
import datetime
import random
import json
import hashlib
import statistics
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

TEST_SIZE=10000
TEST_COUNT=10

from . import *

from ..helpers import calcEtag

class PostgresDB:
    def __init__(self):

        # Generic reusable cursor for normal ops
        self._cur = pg.cursor()


    def drop_schema(self,commit=True):
        self._cur.execute("DROP VIEW IF EXISTS idigbio_uuids_new")
        self._cur.execute("DROP VIEW IF EXISTS idigbio_uuids_data")
        self._cur.execute("DROP TABLE IF EXISTS uuids_data")    
        self._cur.execute("DROP TABLE IF EXISTS uuids")
        self._cur.execute("DROP TABLE IF EXISTS data")

        if commit:
            pg.commit()

    def create_schema(self,commit=True):

        self._cur.execute("""CREATE TABLE IF NOT EXISTS uuids (
            id uuid NOT NULL PRIMARY KEY,
            type varchar(50) NOT NULL,
            parent uuid,
            deleted boolean NOT NULL DEFAULT false
        )""")


        self._cur.execute("""CREATE TABLE IF NOT EXISTS data (
            etag varchar(41) NOT NULL PRIMARY KEY,
            data jsonb
        )""")


        self._cur.execute("""CREATE TABLE IF NOT EXISTS uuids_data (
            id bigserial NOT NULL PRIMARY KEY,
            uuids_id uuid NOT NULL REFERENCES uuids(id),
            data_etag varchar(41) NOT NULL REFERENCES data(etag),
            modified timestamp NOT NULL DEFAULT now(),
            version int NOT NULL DEFAULT 1
        )""")

        self._cur.execute("CREATE INDEX uuids_data_uuids_id ON uuids_data (uuids_id)")
        self._cur.execute("CREATE INDEX uuids_data_modified ON uuids_data (modified)")
        self._cur.execute("CREATE INDEX uuids_deleted ON uuids (deleted)")
        self._cur.execute("CREATE INDEX uuids_parent ON uuids (parent)")
        self._cur.execute("CREATE INDEX uuids_type ON uuids (type)")

        self._cur.execute("""CREATE OR REPLACE VIEW idigbio_uuids_new AS 
            SELECT 
                uuids.id as id,
                type,
                deleted,
                data_etag as etag,
                version,
                modified,
                parent
            FROM uuids 
            LEFT JOIN LATERAL (
                SELECT * FROM uuids_data
                WHERE uuids_id=uuids.id
                ORDER BY modified DESC
                LIMIT 1
            ) AS latest
            ON uuids_id=uuids.id
        """)

        self._cur.execute("""CREATE OR REPLACE VIEW idigbio_uuids_data AS
            SELECT 
                uuids.id as id,
                type,
                deleted,
                data_etag as etag,
                version,
                modified,
                parent,
                data
            FROM uuids 
            LEFT JOIN LATERAL (
                SELECT * FROM uuids_data
                WHERE uuids_id=uuids.id
                ORDER BY modified DESC
                LIMIT 1
            ) AS latest
            ON uuids_id=uuids.id
            LEFT JOIN data
            ON data_etag = etag
        """)

        if commit:
            pg.commit()

    def migrate_data(self,initial=False):
        if initial:
            #Initial Run
            self._cur.execute("""INSERT INTO uuids (id,type,parent,deleted)
                SELECT id,type,parent,deleted FROM idigbio_uuids_bak
            """)
            pg.commit()

            self._cur.execute("""INSERT INTO data (etag)
                SELECT DISTINCT etag FROM idigbio_uuids_bak
            """)
            pg.commit()

            self._cur.execute("""INSERT INTO uuids_data (uuids_id,data_etag,version,modified)
                SELECT id,etag,version,modified FROM idigbio_uuids_bak
            """)
            pg.commit()

        else:
            self._cur.execute("""WITH new_ids AS (
                    SELECT id,type,parent,deleted FROM (
                        SELECT id FROM idigbio_uuids_bak
                        EXCEPT
                        SELECT id FROM uuids
                    ) as idlist NATURAL JOIN idigbio_uuids_bak             
                )
                INSERT INTO uuids (id,type,parent,deleted)
                SELECT * FROM new_ids
            """)
            pg.commit()

            self._cur.execute("""WITH new_etags AS (           
                    SELECT etag as data FROM idigbio_uuids_bak
                    EXCEPT
                    SELECT etag as data FROM data
                )
                INSERT INTO data (etag)
                SELECT * FROM new_etags
            """)
            pg.commit()

            self._cur.execute("""WITH new_versions AS (
                    SELECT idlist.id as uuids_id,idlist.etag as data_etag,idlist.version as version,idigbio_uuids_bak.modified as modified FROM (
                        SELECT id,etag,version FROM idigbio_uuids_bak
                        EXCEPT
                        SELECT uuids_id, data_etag,version FROM uuids_data
                    ) as idlist JOIN idigbio_uuids_bak ON idigbio_uuids_bak.id=idlist.id
                )
                INSERT INTO uuids_data (uuids_id,data_etag,version,modified)
                SELECT * FROM new_versions
            """)
            pg.commit()            

    def __get_ss_cursor(self,name=None):
        """ Get a named server side cursor for large ops"""

        if name is None:
            return pg.cursor(str(uuid.uuid4()))
        else:
            return pg.cursor(name)

    def get_item(self,u,version=None):
        if version is not None:
            # Fetch by version ignores the deleted flag
            self._cur.execute("""SELECT uuids.id,type,deleted,etag,modified,version,parent,data FROM uuids 
                LEFT JOIN uuids_data
                ON uuids_id=uuids.id
                LEFT JOIN data
                ON data_etag = etag
                WHERE uuids.id=%s and version=%s
            """, (u,version))
        else:
            self._cur.execute("""SELECT uuids.id,type,deleted,etag,modified,version,parent,data FROM uuids 
                LEFT JOIN LATERAL (
                    SELECT * FROM uuids_data
                    WHERE uuids_id=uuids.id
                    ORDER BY modified DESC
                    LIMIT 1
                ) AS latest
                ON uuids_id=uuids.id
                LEFT JOIN data
                ON data_etag = etag
                WHERE deleted=false and uuids.id=%s
            """, (u,))
        return self._cur.fetchone()

    def get_type_list(self,t):
        cur = self.__get_ss_cursor()
        cur.execute("""SELECT uuids.id as uuid,type,deleted,data_etag as etag,modified,version,parent FROM uuids 
            LEFT JOIN LATERAL (
                SELECT * FROM uuids_data
                WHERE uuids_id=uuids.id
                ORDER BY modified DESC
                LIMIT 1
            ) AS latest
            ON uuids_id=uuids.id
            WHERE deleted=false and type=%s
        """, (t,))
        for r in cur:
            yield r


    def get_children_list(self,u,t):
        cur = self.__get_ss_cursor()
        cur.execute("""SELECT uuids.id as uuid,type,deleted,data_etag as etag,modified,version,parent FROM uuids 
            LEFT JOIN LATERAL (
                SELECT * FROM uuids_data
                WHERE uuids_id=uuids.id
                ORDER BY modified DESC
                LIMIT 1
            ) AS latest
            ON uuids_id=uuids.id
            WHERE deleted=false and type=%s and parent=%s
        """,(t,u))
        for r in cur:
            yield r

    # UUID
    def __get_upsert_uuid(self):
        return """INSERT INTO uuids (id,type) 
            SELECT %(uuid)s as id, 'records' as type WHERE NOT EXISTS (
                SELECT 1 FROM uuids WHERE id=%(uuid)s
            )
        """

    def __upsert_uuid(self,u,commit=True):
        cur.execute(self.__get_upsert_uuid(), {"uuid": u})
        if commit:
            pg.commit()

    def __upsert_uuid_l(self,ul,commit=True):
        cur.executemany(self.__get_upsert_uuid(), [{"uuid": u} for u in ul])
        if commit:
            pg.commit()

    # DATA
    def __get_upsert_data(self):
        return """INSERT INTO data (etag,data)
            SELECT %(etag)s as etag, %(data)s as data WHERE NOT EXISTS (
                SELECT 1 FROM data WHERE etag=%(etag)s
            )
        """

    def __upsert_data(self,d,commit=True):    
        cur.execute(self.__get_upsert_data(), {"etag": calcEtag(d), "data": json.dumps(d) })
        if commit:
            pg.commit()

    def __upsert_data_l(self,dl,commit=True):
        cur.executemany(self.__get_upsert_data(), [{"etag": calcEtag(d), "data": json.dumps(d) } for d in dl])
        if commit:
            pg.commit()

    # UUID DATA
    def __get_upsert_uuid_data(self):
        return """WITH v AS (
            SELECT * FROM (
                SELECT data_etag, version, modified FROM uuids_data WHERE uuids_id=%(uuid)s 
                UNION 
                SELECT NULL as data_etag, 0 as version, NULL as modified
            ) as sq ORDER BY modified DESC NULLS LAST LIMIT 1
        )
        INSERT INTO uuids_data (uuids_id,data_etag,version)
            SELECT %(uuid)s as uuids_id, %(etag)s as data_etag, v.version+1 as version FROM v WHERE NOT EXISTS (
                SELECT 1 FROM uuids_data WHERE uuids_id=%(uuid)s AND data_etag=%(etag)s AND version=v.version
            )
        """

    def __upsert_uuid_data(self,ud,commit=True):
        cur.execute(self.__get_upsert_uuid_data(), {"uuid": ud["uuid"], "etag": calcEtag(ud["data"])})
        if commit:
            pg.commit()

    def __upsert_uuid_data_l(self,udl,commit=True):
        cur.executemany(self.__get_upsert_uuid_data(), [{"uuid": ud["uuid"], "etag": calcEtag(ud["data"])} for ud in udl])
        if commit:
            pg.commit()