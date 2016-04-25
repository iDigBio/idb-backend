import os
import json

import logging

logger = logging.getLogger('idb.cfg')

conf_paths = ["/etc/idigbio/", "~/", "."]

config = {}

ENV = 'dev'

for p in conf_paths:
    p = os.path.abspath(os.path.expanduser(p)) + "/"
    if os.path.exists(p + "idigbio.json"):
        with open(p + "idigbio.json", "rb") as conf:
            config.update(json.load(conf))

if "env" in config:
    for k in config["env"]:
        if k not in os.environ:
            os.environ[k] = config["env"][k]

ENV = os.environ.get('ENV')
IDB_UUID = os.environ.get('IDB_UUID')
IDB_APIKEY = os.environ.get('IDB_APIKEY')
IDB_DBPASS = os.environ.get('IDB_DBPASS')
IDB_STORAGE_ACCESS_KEY = os.environ.get('IDB_STORAGE_ACCESS_KEY')
IDB_STORAGE_SECRET_KEY = os.environ.get('IDB_STORAGE_SECRET_KEY')
IDB_CRYPT_KEY = os.environ.get('IDB_CRYPT_KEY')
