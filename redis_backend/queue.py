from . import *

import time
import datetime
import dateutil.parser

from collections import defaultdict
import math

class RedisQueue(object):

    def __init__(self,queue_prefix="",stats_interval=10,stats_duration=3600):
        self.p = redist.pubsub()
        self.queue_prefix = queue_prefix
        self.stats_interval = stats_interval
        self.stats_duration = stats_duration
        self.hwm = defaultdict(int)

    def __logitem(self,t):
        n = datetime.datetime.now()
        k = self.queue_prefix + t + "_counter_" + (n - datetime.timedelta(0,(n.minute*60 + n.second)%self.stats_interval,n.microsecond)).isoformat()
        redist.incr(k,1)
        if self.stats_duration is not None:
            redist.expireat(k,n+datetime.timedelta(0,self.stats_duration))

    def list_queues(self):
        return [k[len(self.queue_prefix):-len("_queue")] for k in redist.scan_iter(self.queue_prefix + "*_queue")]

    def listen(self):
        self.p.psubscribe(self.queue_prefix + "*")

        count = 0
        for message in self.p.listen():
            t = message["channel"][len(self.queue_prefix):]
            if message["data"] == "shutdown":
                break
            elif message["data"] == "dump_stats":
                self.stats_report(t)
                continue
            e = redist.spop(self.queue_prefix + t + "_queue")
            if e is not None:
                self.__logitem(t)
                yield (t,e)

    def drain(self,t):
        e = redist.spop(self.queue_prefix + t + "_queue")
        while e is not None:
            self.__logitem(t)
            yield (t,e)
            e = redist.spop(self.queue_prefix + t + "_queue")

    def add(self,t,e):
        redist.sadd(self.queue_prefix + t + "_queue",e)
        redist.publish(self.queue_prefix + t, e)
        hwm = redist.scard(self.queue_prefix + t + "_queue")
        if hwm > self.hwm[t]:
            self.hwm[t] = hwm

    def __len__(self,t):
        return redist.scard(self.queue_prefix + t + "_queue")

    def get_stats(self,t):
        a = []
        for k in redist.scan_iter(self.queue_prefix + t + "_counter_*"):
            t = dateutil.parser.parse(k.split("_")[-1])
            v = int(redist.get(k))
            a.append((t,v))
        mint = min([t for t,_ in a])
        if mint is None:
            return []

        filled_a = []
        if self.stats_duration is not None:
            for t in [mint + datetime.timedelta(0,x) for x in range(0,self.stats_duration,self.stats_interval)]:
                if t > datetime.datetime.now():
                    break
                for ts,v in a:
                    if t == ts:
                        filled_a.append((t,v))
                        break
                else:
                    filled_a.append((t,0))
        else:
            t = mint
            while t < datetime.datetime.now():
                for ts,v in a:
                    if t == ts:
                        filled_a.append((t,v))
                        break
                else:
                    filled_a.append((t,0))
                t += datetime.timedelta(0,self.stats_interval)

        return filled_a

    def stats_report(self,t):
        s = self.get_stats(t)
        sv = sorted([x[1] for x in s])
        print "S: ", s[0][0].isoformat()
        print "INT: ", (s[-1][0] - s[0][0]).total_seconds()
        print "CNT: ", len(sv)
        print "MIN: ", min(sv)
        print "MAX: ", max(sv)
        print "AVG: ", sum(sv)/float(len(sv))
        if len(sv) % 2 == 0:
            print "MED: ", sv[len(sv)/2]
        else:
            print "MED: ", sv[int(math.floor(len(sv)/2.0)):int(math.ceil(len(sv)/2.0))+1]

        print "HWM: ", dict(self.hwm)

def main():
    # q = RedisQueue("testing_queue_",1,30)

    # tt = "test"

    # for x in xrange(0,100000):
    #     q.add(tt,x)

    # for v in q.drain(tt):
    #     pass

    # q.stats_report(tt)
    q = RedisQueue("cacher_")
    q.stats_report("records")
    q.stats_report("mediarecords")


if __name__ == '__main__':
    main()