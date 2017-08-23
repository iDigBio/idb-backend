import requests
from gevent.pool import Pool
import jsonlines

LIMIT = 1000

s = requests.Session()

def urls(total):
    for x in xrange(0, total, LIMIT):
        yield "https://api.epandda.org/annotations?offset={}&limit={}".format(x, LIMIT)

def get_url(u):
    r = s.get(u)
    r.raise_for_status()

    return r.json()["results"]

def main():
    p = Pool(20)

    r = s.get("https://api.epandda.org/annotations?limit=1")
    r.raise_for_status()

    total = r.json()["counts"]["totalCount"]
    print(total)

    fetched = 0

    result_iter = p.imap(get_url, urls(total))
    with jsonlines.open("/home/godfoder/Downloads/annotations.jsonl", mode="w") as writer:
        for res in result_iter:
            fetched += len(res)
            writer.write_all(res)

            if fetched % 10000 == 0:
                print(fetched)

    print(fetched)


if __name__ == '__main__':
    main()
