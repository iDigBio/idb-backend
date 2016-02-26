import random
import datetime

from idb.helpers.rg import ReverseGeocoder

t = datetime.datetime.now()
rg = ReverseGeocoder()
print "INIT Time", (datetime.datetime.now() - t).total_seconds()*1000

x = []

NUM_POINTS = 1000000
NUM_ITERATIONS = 10

for _ in range(0,NUM_POINTS):
    x.append([random.randrange(-9000,9000)/100.0,random.randrange(-18000,18000)/100.0])

times = []

for _ in range(0,NUM_ITERATIONS):
    for c in x:
        t = datetime.datetime.now()
        rg.get_country(c[1],c[0])
        times.append((datetime.datetime.now() - t).total_seconds()*1000)

times = sorted(times)

c = len(times)
mn = times[0]
q1 = times[int(c/4)]
q2 = times[int(c/2)]
q3 = times[int(3*c/4)]
p95 = times[int(95*c/100)]
p99 = times[int(99*c/100)] 

iqr = q3-q1
mx = times[-1]
sm = sum(times)
av = sm/len(times)

print mn, q1, q2, q3, mx
print sm, av, iqr, p95, p99
