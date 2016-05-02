import os
import sys
import json

from idb.postgres_backend.stats_db import statsdbpool, DictCursor

filter_by = {
    "taxon": [
        "kingdom",
        "phylum",
        "class",
        "order",
        "family",
        "genus",
        "specificepithet",
        "scientificname",
    ]
}

def get_path(path, d):
    r = d
    try:
        for k in path:
            r = r[k]
    except:
        return None
    return r

def format_row(r):
    q_o = json.loads(r["query"])
    qt = ""
    ft = get_path(["filtered","query","match","_all","query"],q_o)
    if ft is not None:
        qt += "Full Text: {0}, ".format(ft)

    and_block = get_path(["filtered","filter","and"],q_o)
    if and_block is not None:
        for b in and_block:
            if "term" in b:
                k = b["term"].keys()[0]
                qt += "{0}={1}, ".format(k, b["term"][k])
            elif "terms" in b:
                del b["terms"]["execution"]
                k = b["terms"].keys()[0]
                qt += "{0}={1}, ".format(k, b["terms"][k])
            elif "exists" in b:
                qt += "{0} is present, ".format(b["exists"]["field"])
            elif "missing" in b:
                qt += "{0} is absent, ".format(b["missing"]["field"])
            else:
                qt += json.dumps(b) + ", "

    if qt == "":
        qt = r["query"]
    elif qt.endswith(", "):
        qt = qt[:-2]

    return "\t".join([str(r["id"]), qt, str(r["count"])]) + "\n"

def get_queries(recordset, filt="taxon"):
    sql = ("""select queries.id, queries.query, count(*) as count
                from stats
                join queries on stats.query_id=queries.id
                where type='search' and payload ? %s
                group by queries.id
                order by count(*);""",
           [recordset])

    with open(recordset + "_report.tsv", "wb") as outf:
        outf.write("queryID\tquery\tcount\n")
        for r in statsdbpool.fetchiter(*sql, cursor_factory=DictCursor):
            if filt is not None:
                for t in filter_by[filt]:
                    if t in r["query"]:
                        outf.write(format_row(r))
                        break
            else:
                outf.write(format_row(r))

def main():
    recordset = sys.argv[1]
    get_queries(recordset)

if __name__ == '__main__':
    main()
