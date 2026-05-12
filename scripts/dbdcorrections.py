import json
import bsddb3

DB_PATH = "/tmp/example.db"

db_env = bsddb3.db.DBEnv()
db_env.open(None, bsddb3.db.DB_CREATE)

db_handle = bsddb3.db.DB(db_env)
db_handle.open(DB_PATH, None, bsddb3.db.DB_HASH, bsddb3.db.DB_CREATE)

def b(x) -> bytes:
    # Convert ints/str/anything JSON-y into bytes deterministically
    if isinstance(x, bytes):
        return x
    if isinstance(x, str):
        return x.encode("utf-8")
    # for ints, dicts, lists, etc.
    return json.dumps(x, ensure_ascii=False, separators=(",", ":")).encode("utf-8")

def insert_value(key, value):
    k = b(key)
    v = b(value)

    existing = db_handle.get(k)
    if existing is None:
        db_handle[k] = v
    else:
        db_handle[k] = existing + b"\n" + v
        print(f"multiple keys added for: {key}")

with open("./collectionsKV.json", "r", encoding="utf-8") as json_file:
    for line in json_file:
        json_data = json.loads(line)
        for key, value in json_data.items():
            insert_value(key, value)

db_handle.close()
db_env.close()
