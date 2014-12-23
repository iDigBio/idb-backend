from postgres_backend import pg, DictCursor

import uuid
import json
import itertools
import datetime



def main():
    s = "0123456789abcdef"
    splits = {
        2: ["".join(list(c)) for c in itertools.product(s,repeat=2)],
        3: ["".join(list(c)) for c in itertools.product(s,repeat=3)]
    }

    splitsize = 2

    t = datetime.datetime.now()

    for shard in splits[splitsize][1:]:
        fulluuid = None
        if splitsize == 2:
            fulluuid = shard+"000000-0000-0000-0000-000000000000"
        elif splitsize == 3:
            fulluuid = shard+"00000-0000-0000-0000-000000000000"

        cursor = pg.cursor(str(uuid.uuid4()),cursor_factory=DictCursor)

        cursor.execute("select cache.id as id,min(data::text)::json as data,json_agg(v) as values from cache join corrections on data @> k where cache.id < %s group by cache.id", (fulluuid,))

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

            if count % 10000 == 0:
                print count, startkeys/count, endkeys/count, datetime.datetime.now() - t

        print count, startkeys/count, endkeys/count, datetime.datetime.now() - t

        break

    # Last records

    # fulluuid = None
    # if splitsize == 2:
    #     fulluuid = shard+"000000-0000-0000-0000-000000000000"
    # elif splitsize == 3:
    #     fulluuid = shard+"00000-0000-0000-0000-000000000000"

    # cursor = pg.cursor(str(uuid.uuid4()),cursor_factory=DictCursor)

    # cursor.execute("select cache.id as id,min(data::text)::json as data,json_agg(v) as values from cache join corrections on data @> k where cache.id < %s group by cache.id", (fulluuid,))

    # count = 0.0
    # startkeys = 0
    # endkeys = 0

    # for r in cursor:
    #     count += 1.0
    #     d = r["data"]["idigbio:data"]
    #     startkeys += len(d.keys())

    #     for v in r["values"]:
    #         d.update(v)
        
    #     endkeys += len(d.keys())

    #     if count % 10000 == 0:
    #         print count, startkeys/count, endkeys/count, t - datetime.datetime.now()

    # print count, startkeys/count, endkeys/count, t - datetime.datetime.now()


if __name__ == '__main__':
    main()
