import requests
import json

from idb.postgres_backend import apidbpool, cursor
from idb.postgres_backend.db import PostgresDB


def get_active_rsids(since=None):
    sql = """
        SELECT uuid
        FROM recordsets
        WHERE ingest=true
          AND uuid IS NOT NULL
          AND file_harvest_date IS NOT NULL
    """
    params = []
    if since:
        sql += "AND file_harvest_date >= %s"
        params.append(since)
    sql += "ORDER BY file_harvest_date DESC"
    return [r[0] for r in apidbpool.fetchall(sql, params, cursor_factory=cursor)]


def main3():
    for rsid in get_active_rsids():
        print(rsid)

def main2():
    for r in PostgresDB.get_type_list("recordset", limit=None):
        try:
            print r["uuid"]
        except:
            traceback.print_exc()

def main1():
    r = requests.get("http://search.idigbio.org/v2/search/recordsets/", params={"rsq":json.dumps({"data.ingest":True}),"fields":json.dumps(["uuid"]),"limit":5000})
    r.raise_for_status()
    o = r.json()

    for rs in o["items"]:
        print rs["uuid"]

if __name__ == '__main__':
    main3()
