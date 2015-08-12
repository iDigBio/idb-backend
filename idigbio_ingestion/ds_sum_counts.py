import json
import os
import sys

base = sys.argv[1]

rt = 0
mt = 0
rc = 0
mc = 0
ru = 0
mu = 0 

header = ["file","csv line count","record count","record create","record update","record delete", "mediarecord count", "mediarecord create","mediarecord update","mediarecord delete","duplicated occurence ids", "no recordid count"]
fields = ["csv_line_count", "records_count", "records_create", "records_update", "records_delete", "mediarecords_count", "mediarecords_create", "mediarecords_update", "mediarecords_delete", "duplicate_occurence_count", "no_recordid_count"]
d = {}

for f in os.listdir(base):
    if f.endswith(".summary.json"):
        o = None
        with open(base + f,"rb") as fp:
	    o = json.load(fp)
            d[o["filename"]] = [o[f] if f in o else 0 for f in fields]
            
print ",".join(header)
totals = [0 for i in range(0,len(header)-1)]
for k in sorted(d.keys()):
    for i,v in enumerate(d[k]):
        totals[i] += v 
    print k + "," + ",".join([ str(x) for x in d[k]])

print "totals," + ",".join([ str(x) for x in totals])
