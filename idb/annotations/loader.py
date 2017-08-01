
from idb.postgres_backend import apidbpool, DictCursor

import json


class AnnotationsLoader(object):
    def __init__(self):
        self.conn = apidbpool.get()
        self.cursor = self.conn.cursor(cursor_factory=DictCursor)
        self.corrections = []

    def __enter__(self):
        self.cursor.execute("BEGIN")
        self.corrections = []
        return self

    def commit(self):
        self.cursor.executemany(
            "INSERT INTO annotations (uuids_id,source_id,v,source,approved) VALUES (%s,%s,%s,%s,%s)",
            self.corrections)
        self.conn.commit()

    def __exit__(self, type, value, traceback):
        self.commit()

    def add_corrections_iter(self, corr_iter):
        def _format(ci):
            for v, approved in ci:
                yield (v["hasTarget"]["@id"].split(":")[-1], v["@id"].split(":")[-1], json.dumps(v).lower(), v["annotatedBy"]["name"], approved)

        self.cursor.executemany(
            "INSERT INTO annotations (uuids_id,source_id,v,source,approved) VALUES (%s,%s,%s,%s,%s)",
            _format(corr_iter)
        )
        self.conn.commit()

    def add_corrections(self, v, approved=False):
        self.corrections.append((v["hasTarget"]["@id"].split(":")[-1], v["@id"].split(":")[-1], json.dumps(v).lower(), v["annotatedBy"]["name"], approved))

    def clear_source(self, source):
        self.cursor.execute(
            "DELETE from annotations WHERE source=%s", (source,))
        self.conn.commit()

    def __del__(self):
        apidbpool.put(self.conn)
        self.conn = None
