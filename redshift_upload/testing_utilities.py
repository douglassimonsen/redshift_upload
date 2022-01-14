import psycopg2
import logging
from typing import List, Dict

try:
    from .credential_store import credential_store
except ImportError:
    from credential_store import credential_store
log = logging.getLogger("redshift_utilities-test")

credential_store.set_store("test-library")


def drop_tables(tables: List[str], aws_creds: Dict = None) -> None:
    """
    Tries to drop a table in a list and all their dependencies
    """
    if aws_creds is None:
        aws_creds = credential_store.credentials()
    if isinstance(tables, str):
        tables = [tables]

    with psycopg2.connect(
        **aws_creds["db"],
        connect_timeout=60,
    ) as conn:
        cursor = conn.cursor()
        for table in tables:
            log.info(f"Beginning to drop table: {table}")
            cursor.execute(f"drop table if exists public.{table} cascade")
            log.info(f"Dropped table: {table}")
        conn.commit()
    log.info("Table dropping completed")
