from postgres_backend import pg, DictCursor

import uuid
import json

def main():
    cursor = pg.cursor(str(uuid.uuid4()),cursor_factory=DictCursor)

    cursor.execute("select cache2.id as id,min(data::text)::json as data,json_agg(v) as values from cache2 join corrections on data @> k group by cache2.id")

    count = 0.0
    startkeys = 0
    endkeys = 0

    for r in cursor:
        count += 1.0
        d = r["data"]["idigbio:data"]
        startkeys += len(d.keys())

        for v in r["values"]:
            d.update(v)
        
        endkeys += len(d.keys())

    print count, startkeys/count, endkeys/count

if __name__ == '__main__':
    main()