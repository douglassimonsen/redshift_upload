import pytest
import psycopg2
import os
import json


def kill_tables(tables, aws_creds):
    with psycopg2.connect(
        host=aws_creds['host'],
        dbname=aws_creds['dbname'],
        port=aws_creds['port'],
        user=aws_creds['redshift_username'],
        password=aws_creds['redshift_password'],
        connect_timeout=180,
    ) as conn:
        cursor = conn.cursor()
        for table in tables:
            cursor.execute(f"drop table if exists public.{table} cascade")
        conn.commit()


def main(coverage=False):
    os.chdir(os.path.dirname(os.path.abspath(__file__)))
    with open("aws_creds.json") as f:
        aws_creds = json.load(f)
    pytest.main([
        'base_utilities/change_directory.py',
        "full_tests/add_column.py",
        "full_tests/column_expansion.py",
        "full_tests/drop_table.py",
        "full_tests/incompatible_types.py",
        "full_tests/load_from_string.py",
        # "full_tests/load_to_locked_table.py",  # currently broken
        "full_tests/no_data.py",
        "full_tests/renamed_columns.py",
        "full_tests/table_view_name_collision.py",
        "full_tests/truncate_table.py",
        "full_tests/upload_in_parallel.py",
        "full_tests/upload_with_view.py",
        "local_utilities/check_coherence.py",
        "local_utilities/fix_column_types_defined.py",
        "local_utilities/fix_column_types_undefined.py",
        "local_utilities/load_source.py",
        "local_utilities/stringify_columns.py",
    ])

    kill_tables([
        "unit_test_column_expansion",
        "unit_test_simple_upload_drop_table",
        "unit_test_simple_upload_truncate_table",
        "unit_test_upload_with_view",
    ], aws_creds)


if __name__ == "__main__":
    main(coverage=True)
