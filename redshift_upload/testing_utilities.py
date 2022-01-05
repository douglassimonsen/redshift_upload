import psycopg2
import logging

try:
    from . import credential_store
except:
    import credential_store
log = logging.getLogger("redshift_utilities-test")

credential_store.set_store("test-library")


def drop_tables(tables, aws_creds=None):

    if aws_creds is None:
        aws_creds = credential_store.credentials()
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
