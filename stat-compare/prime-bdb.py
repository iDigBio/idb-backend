#!/usr/bin/env python3
import os
import bsddb3.db as db


def b(x):
    """Convert str/bytes to bytes for bsddb3."""
    if isinstance(x, bytes):
        return x
    return str(x).encode("utf-8")


env_dir = "testdb_env"
os.makedirs(env_dir, exist_ok=True)

# Step 1: Create the DB environment
env = db.DBEnv()
env.open(
    env_dir,
    db.DB_CREATE
    | db.DB_INIT_MPOOL  # Memory pool (backed by __db.* files)
    | db.DB_INIT_LOCK   # Locking (optional)
    | db.DB_INIT_TXN    # Transactions (optional)
    | db.DB_INIT_LOG    # Logging (optional)
)

# Step 2: Create a database within that environment
database = db.DB(env)
database.open(
    "testdb.db",
    None,
    db.DB_BTREE,
    db.DB_CREATE
)

# Insert a record to cause allocation (bytes in Py3)
database.put(b("key1"), b("value1"))

# Clean up
database.close()
env.close()
