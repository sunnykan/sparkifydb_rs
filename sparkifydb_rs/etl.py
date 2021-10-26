import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):"
    """Copy data from S3 bucket into staging tables"""

    for query in copy_table_queries:
        print(f"Loading staging table: {query}")
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """Insert data into tables: one fact and four dimension tables"""

    for query in insert_table_queries:
        print(f"Loading data table: {query}")
        cur.execute(query)
        conn.commit()


def main():
    """Read config parameters and connect to Redshift database.
    Call functions to load staging tables and populate data tables.
    """
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

    load_staging_tables(cur, conn)
    insert_tables(cur, conn)
    print("All data has been inserted!")

    conn.close()


if __name__ == "__main__":
    main()
