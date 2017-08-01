from __future__ import absolute_import
from idb.postgres_backend import apidbpool, NamedTupleCursor

import uuid
# import copy
# import traceback

# from idb.helpers.etags import objectHasher

class AnnotationApplicator(object):
    corrections = None
    keytups = None

    def __init__(self, reload=True):
        if reload:
            self.reload()

    def reload(self):
        # sql = "select k::json,v::json from corrections"
        # self.keytups = set()

        # self.corrections = {}
        # for r in apidbpool.fetchiter(sql, name=str(uuid.uuid4()), cursor_factory=NamedTupleCursor):
        #     uk = tuple(r.k.keys())
        #     self.keytups.add(uk)

        #     etag = objectHasher("sha256", r.k)

        #     self.corrections[etag] = r.v
        pass

    def create_schema(self):
        with apidbpool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(""" CREATE TABLE IF NOT EXISTS annotations (
                        id bigserial PRIMARY KEY,
                        source_id uuid NOT NULL,
                        uuids_id uuid NOT NULL REFERENCES uuids(id),
                        v jsonb NOT NULL,
                        approved boolean NOT NULL DEFAULT false,
                        source varchar(50) NOT NULL,
                        updated_at timestamp DEFAULT now()
                    )
                """)
                conn.commit()
                # try:
                #     cur.execute("CREATE INDEX corrections_source ON corrections (source)")
                #     conn.commit()
                # except:
                #     pass

    def get_annotations(u):
        annotations = []
        for r in apidbpool.fetchiter("select * from annotations where uuids_id='{}'", name=str(uuid.uuid4()), cursor_factory=NamedTupleCursor):
            annotations.append(r)
        return annotations
