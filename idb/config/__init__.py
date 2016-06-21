import os
import json
import logging

logger = logging.getLogger('idb.cfg')

conf_paths = ["/etc/idigbio/", "~/", "."]

config = {}

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
    fp = os.path.join(os.path.abspath(os.path.expanduser(p)), "idigbio.json")
    try:
        load_config_file(fp)
    except IOError:
        pass


ENV = os.environ.get('ENV', 'dev')
IDB_UUID = os.environ.get('IDB_UUID')
IDB_APIKEY = os.environ.get('IDB_APIKEY')
IDB_DBPASS = os.environ.get('IDB_DBPASS')
IDB_STORAGE_ACCESS_KEY = os.environ.get('IDB_STORAGE_ACCESS_KEY')
IDB_STORAGE_SECRET_KEY = os.environ.get('IDB_STORAGE_SECRET_KEY')
IDB_CRYPT_KEY = os.environ.get('IDB_CRYPT_KEY')
