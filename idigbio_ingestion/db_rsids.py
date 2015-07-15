import requests
import json


from idb.postgres_backend.db import PostgresDB

def main2():
    db = PostgresDB()
    for r in db.get_type_list("recordset", limit=None):
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
    main1()