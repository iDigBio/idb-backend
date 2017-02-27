import uuid
import hashlib
import string
import random

from idb.postgres_backend.db import PostgresDB
from idb.helpers.encryption import _encrypt

from idb import config

db = PostgresDB()

def getCode(length=100, char=None):
    if char is None:
        char = string.ascii_letters + string.digits
    return ''.join(random.choice(char) for x in range(length))


user_uuid = str(uuid.uuid4())

m = hashlib.md5()
m.update(getCode())
api_key = m.hexdigest()

api_key_e = _encrypt(api_key, config.IDB_CRYPT_KEY)

feeder_parent_id = "520dcbb3-f35a-424c-8778-6df11afc9f95"
rs_rid = "http://feeder.idigbio.org/datasets/{0}".format(user_uuid)

db.execute("INSERT INTO idb_api_keys (user_uuid,apikey) VALUES (%s,%s)",
           (user_uuid, api_key_e))
db.set_record(user_uuid, "recordset", feeder_parent_id, {}, [rs_rid], [])
db.commit()

print "#########################################################"
print "Environment (ENV):", config.ENV
print "UUID:", user_uuid
print "API Key:", api_key
print "Appliance Recordset RecordID:", rs_rid
print "#########################################################"
