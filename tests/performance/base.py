import re
import sys
import pathlib
sys.path.insert(0, str(pathlib.Path(__file__).parents[1]))
from redshift_upload import upload, base_utilities  # noqa
from redshift_upload.db_interfaces import redshift
import json  # noqa
with base_utilities.change_directory():
    with open("../tests/aws_creds.json") as f:
        aws_creds = json.load(f)
df = [{'a': 'hi' * 1000} for _ in range(1_000_000)]
query = '''
drop table if exists public.perf_test;
create table public.perf_test (a varchar(2000));
truncate table public.perf_test;
'''
with redshift.Interface("public", "perf_test", aws_creds).get_db_conn() as conn:
    cursor = conn.cursor()
    cursor.execute(query)
    conn.commit()



def base():
    upload(
        source=df,
        schema_name="public",
        table_name="perf_test",
        upload_options={
            "skip_checks": True,
        },
        aws_info=aws_creds
    )


if __name__ == '__main__':
    base()
