import sys
import pathlib
sys.path.insert(0, str(pathlib.Path(__file__).parents[2]))
from redshift_upload import upload, base_utilities  # noqa
import json  # noqa
import datetime  # noqa
with base_utilities.change_directory():
    with open("../aws_creds.json") as f:
        aws_creds = json.load(f)

today = datetime.datetime.today()
today_date = today.date()
df1 = [{"a": 1, "b": "hi"}, {"a": 2}, {"a": 3}]
df2 = [{"a": 1}, {"a": 2}, {"a": 3}]


def test_drop_table():
    upload(
        source=df1,  # needed for the comparison later
        schema_name="public",
        table_name="unit_test_simple_upload_incompatible_types",
        upload_options={"drop_table": True},
        aws_info=aws_creds
    )
    upload(
        source=df2,  # needed for the comparison later
        schema_name="public",
        table_name="unit_test_simple_upload_incompatible_types",
        aws_info=aws_creds
    )


if __name__ == '__main__':
    test_drop_table()
