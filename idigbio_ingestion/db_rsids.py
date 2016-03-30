import requests
import json


from idb.postgres_backend.db import PostgresDB

def main3():
    db = PostgresDB()
    sql = "SELECT * FROM recordsets WHERE ingest=true and uuid IS NOT NULL AND file_harvest_date IS NOT NULL ORDER BY file_harvest_date"

    for r in db.fetchall(sql):
        try:
            print r["uuid"]
        except:
            traceback.print_exc()

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
