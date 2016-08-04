
from idb.postgres_backend import apidbpool, DictCursor

import json


class CorrectionsLoader(object):
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
            "INSERT INTO corrections (k,v,source,approved) VALUES (%s,%s,%s,%s)",
            self.corrections)
        self.conn.commit()

    def __exit__(self, type, value, traceback):
        self.commit()

    def add_corrections_iter(self, corr_iter):
        def _format(ci):
            for k, v, source, approved in ci:
                yield (json.dumps(k).lower(), json.dumps(v).lower(), source, approved)

        self.cursor.executemany(
            "INSERT INTO corrections (k,v,source,approved) VALUES (%s,%s,%s,%s)",
            _format(corr_iter)
        )
        self.conn.commit()

    def add_corrections(self, k, v, source, approved=False):
        self.corrections.append(
            (json.dumps(k).lower(), json.dumps(v).lower(), source, approved))

    def clear_source(self, source):
        self.cursor.execute(
            "DELETE from corrections WHERE source=%s", (source,))
        self.conn.commit()

    def __del__(self):
        apidbpool.put(self.conn)
        self.conn = None
