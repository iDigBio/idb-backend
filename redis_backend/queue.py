from . import *

import time

class RedisQueue(object):

    def __init__(queue_prefix=""):
        self.p = redist.pubsub()


    def listen(sleep_time=1):
        self.p.psubscribe(queue_prefix + "*")

        count = 0
        for message in p.listen():
            t = message["channel"][len(queue_prefix):]
            if message["data"] == "shutdown":
                break
            e = redist.spop(queue_prefix + t + "_queue")
            if e is not None:
                yield (t,e)
            else:
                time.sleep(sleep_time)

    def add(t,e):
        redist.sadd(queue_prefix + t + "_queue",e)
        redist.publish(queue_prefix + t, e)