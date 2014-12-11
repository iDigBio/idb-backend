import redis

from config import config

redist = redis.StrictRedis(**config["redis"])