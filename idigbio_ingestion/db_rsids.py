from idb.postgres_backend.db import PostgresDB

def main():
    db = PostgresDB()
    for r in db.get_type_list("recordset", limit=None):
        try:
            print r["uuid"]
        except:
            traceback.print_exc()


if __name__ == '__main__':
    main()