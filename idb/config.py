import os
import json
import logging

logger = logging.getLogger('idb.cfg')

conf_paths = ["/etc/idigbio/", "~/", "."]

config = {
    "postgres": {
        "host": "localhost",
        "user": "idigbio",
        "password": "",
        "db_prefix": "idb_"
    },
    "elasticsearch": {
        "types": [
            "publishers",
            "recordsets",
            "mediarecords",
            "records"
        ],
        "indexname": "idigbio",
        "servers": []
    }
}

def update_environment(env):
    if not env:
        return
    os.environ.update(env)
    for k,v in env.items():
        globals()[k] = v

def load_config_file(p):
    with open(p, "rb") as conf:
        logger.debug("Reading config from %r", p)
        json_config = json.load(conf)
        config.update(json_config)
        update_environment(json_config.get('env'))


for p in conf_paths:
    fp = os.path.realpath(os.path.join(os.path.expanduser(p), "idigbio.json"))
    try:
        load_config_file(fp)
    except IOError:
        pass

# These become available as config.VARIABLE when imported e.g. 'from idb import config'
ENV = os.environ.get('ENV', 'dev')
IDB_UUID = os.environ.get('IDB_UUID')
IDB_APIKEY = os.environ.get('IDB_APIKEY')
IDB_DBPASS = os.environ.get('IDB_DBPASS')
IDB_STORAGE_HOST = os.environ.get('IDB_STORAGE_HOST', '172.17.0.3')
IDB_STORAGE_ACCESS_KEY = os.environ.get('IDB_STORAGE_ACCESS_KEY')
IDB_STORAGE_SECRET_KEY = os.environ.get('IDB_STORAGE_SECRET_KEY')
IDB_CRYPT_KEY = os.environ.get('IDB_CRYPT_KEY')

ES_ALLOW_INDEX_CREATION = os.environ.get('ES_ALLOW_INDEX_CREATION', 'yes')
ES_INDEX_CHUNK_SIZE = os.environ.get('ES_INDEX_CHUNK_SIZE', '500')
ES_INDEX_NUMBER_OF_SHARDS = os.environ.get('ES_INDEX_NUMBER_OF_SHARDS', '48')
ES_INDEX_NUMBER_OF_REPLICAS = os.environ.get('ES_INDEX_NUMBER_OF_REPLICAS', '23')
ES_INDEX_REFRESH_INTERVAL = os.environ.get('ES_INDEX_REFRESH_INTERVAL', '1s')

IDB_EXTRA_SERIOUS_DEBUG = os.environ.get('IDB_EXTRA_SERIOUS_DEBUG', 'no')
