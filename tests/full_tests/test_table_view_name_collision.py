import sys
import pathlib
sys.path.insert(0, str(pathlib.Path(__file__).parents[2]))
import pytest
from redshift_upload import upload, base_utilities  # noqa
import json  # noqa
with base_utilities.change_directory():
    with open("../aws_creds.json") as f:
        aws_creds = json.load(f)
df = [{"a": 1}, {"a": 2}, {"a": 3}]




def test_drop_table():
    interface = upload(
        source=df,  # needed for the comparison later
        schema_name="public",
        table_name="dummy",
        upload_options={"drop_table": True, "close_on_end": False},
        aws_info=aws_creds
    )
    with interface.get_db_conn() as conn:
        conn.cursor().execute(f"create or replace view dummy2 as (select * from dummy)")
        conn.commit()
    with pytest.raises(ValueError):
        interface = upload(
            source=df,  # needed for the comparison later
            schema_name="public",
            table_name="dummy2",
            upload_options={"drop_table": True},
            aws_info=aws_creds
        )


if __name__ == '__main__':
    test_drop_table()
