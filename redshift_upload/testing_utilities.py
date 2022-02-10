import psycopg2
import logging
from typing import List, Dict
import json

try:
    from .credential_store import credential_store
except ImportError:
    from credential_store import credential_store
log = logging.getLogger("redshift_utilities-test")

credential_store.set_store("test-library")

field_types_query = """
set search_path to {schema_name};
select "type"
from pg_table_def

where schemaname = '{schema_name}'
and tablename = '{table_name}';
"""


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


def compare_sources(local_data, remote, conn, field_types=None):
    def stringify(table):
        table = [{k: str(v) for k, v in row.items()} for row in table]
        return json.dumps(
            list(
                sorted(table, key=lambda x: json.dumps(x, sort_keys=True, default=str))
            ),
            sort_keys=True,
            default=str,
            indent=2,
        )

    def get_remote():
        cursor = conn.cursor()
        cursor.execute(f"select * from {remote}")
        rows = cursor.fetchall()
        columns = [x[0] for x in cursor.description]
        rows = [dict(zip(columns, row)) for row in rows]
        return stringify(rows)

    def check_types():
        cursor = conn.cursor()
        schema, table = remote.split(".")
        cursor.execute(field_types_query.format(schema_name=schema, table_name=table))
        col_types = cursor.fetchall()
        col_types = [x[0] for x in col_types]
        if col_types != field_types:
            print(
                [
                    [i, col_type, field_type]
                    for i, (col_type, field_type) in enumerate(
                        zip(col_types, field_types)
                    )
                    if col_type != field_type
                ]
            )
            raise AssertionError("Column Type Issue")

    if field_types is not None:
        check_types()

    remote_vals = get_remote()
    local_vals = stringify(local_data)

    if remote_vals != local_vals:
        remote_data = json.loads(remote_vals)[0]
        local_data = json.loads(local_vals)[0]
        for (col, local), remote in zip(local_data.items(), remote_data.values()):
            if local != remote:
                print(f"col: {col}; local: {local}; remote: {remote}")
        raise AssertionError("Column Value Issue")
