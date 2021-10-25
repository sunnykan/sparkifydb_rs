import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):

    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    config = configparser.ConfigParser()
    config.read("dwh.cfg")

    host, dbname, user, password, port = (
        config.get("DWH", "DWH_HOST"),
        config.get("DWH", "DWH_DB"),
        config.get("DWH", "DWH_DB_USER"),
        config.get("DWH", "DWH_DB_PASSWORD"),
        config.get("DWH", "DWH_PORT"),
    )

    param_string = (
        f"host={host} dbname={dbname} user={user} password={password} port={port}"
    )
    conn = psycopg2.connect(param_string)
    cur = conn.cursor()

    drop_tables(cur, conn)
    create_tables(cur, conn)
    print("All tables have been created!")

    conn.close()


if __name__ == "__main__":
    main()
