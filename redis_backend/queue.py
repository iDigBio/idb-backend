from . import *

import time

class RedisQueue(object):

    def __init__(self,queue_prefix=""):
        self.p = redist.pubsub()
        self.queue_prefix = queue_prefix

    def listen(self,sleep_time=1):
        self.p.psubscribe(self.queue_prefix + "*")

        count = 0
        for message in p.listen():
            t = message["channel"][len(self.queue_prefix):]
            if message["data"] == "shutdown":
                break
            e = redist.spop(self.queue_prefix + t + "_queue")
            if e is not None:
                yield (t,e)
            else:
                time.sleep(sleep_time)

    def add(self,t,e):
        redist.sadd(self.queue_prefix + t + "_queue",e)
        redist.publish(self.queue_prefix + t, e)