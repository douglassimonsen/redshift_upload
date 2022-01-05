import psycopg2
import json
import logging

try:
    from . import base_utilities
except:
    import base_utilities
log = logging.getLogger("redshift_utilities-test")


with base_utilities.change_directory():
    with open("../tests/aws_creds.json") as f:
        default_aws_creds = json.load(f)


def drop_tables(tables, aws_creds=None):

    if aws_creds is None:
        aws_creds = default_aws_creds
    if isinstance(tables, str):
        tables = [tables]

    with psycopg2.connect(
        host=aws_creds["host"],
        dbname=aws_creds["dbname"],
        port=aws_creds["port"],
        user=aws_creds["redshift_username"],
        password=aws_creds["redshift_password"],
        connect_timeout=60,
    ) as conn:
        cursor = conn.cursor()
        for table in tables:
            log.info(f"Beginning to drop table: {table}")
            cursor.execute(f"drop table if exists public.{table} cascade")
            log.info(f"Dropped table: {table}")
        conn.commit()
    log.info("Table dropping completed")
