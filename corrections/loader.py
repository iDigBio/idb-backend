from . import *

import json
import psycopg2.extras

class CorrectionsLoader(object):

    def __init__(self):
        self.cursor = pg.cursor(cursor_factory=DictCursor)
        self.corrections = []

    def __enter__(self):
        self.cursor.execute("BEGIN")
        self.corrections = []
        return self

    def commit(self):
        self.cursor.executemany("INSERT INTO corrections (k,v,approved,source) VALUES (%s,%s,%s,%s)", self.corrections)       
        pg.commit()

    def __exit__(self, type, value, traceback):
        self.commit()

    def add_corrections(self,k,v,source,approved=False):
        self.corrections.append((json.dumps({"idigbio:data": k}).lower(),json.dumps(v).lower(),approved,source))

    def clear_source(self,source):
        self.cursor.execute("DELETE from corrections WHERE source=%s", (source,))
        pg.commit()