import json
import bsddb3

db_env = bsddb3.db.DBEnv()
db_env.open(None, bsddb3.db.DB_CREATE)

db_handle = bsddb3.db.DB(db_env)

def insert_value(key, value):
    existing_value = db_handle.get(str(key))
    if existing_value is None:
        existing_value = str(value)
    else:
        existing_value += b'\n' + str(value)
        print("multiple keys added for: {0} ").format(str(key))

    db_handle[str(key)] = existing_value


db_handle.open("/tmp/example.db", None, bsddb3.db.DB_HASH, bsddb3.db.DB_CREATE)

with open('./collectionsKV.json', 'r') as json_file:
    for line in json_file:
        json_data = json.loads(line)
        for key, value in json_data.items():
            insert_value(key, value)
