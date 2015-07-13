import requests
import json

def main():
    r = requests.get("http://search.idigbio.org/v2/search/recordsets/", params={"rsq":json.dumps({"data.ingest":True}),"fields":json.dumps(["uuid"])})
    r.raise_for_status()
    o = r.json()

    for rs in o["items"]:
        print rs["uuid"]

if __name__ == '__main__':
    main()