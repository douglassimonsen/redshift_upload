import sys
import pathlib
sys.path.insert(0, str(pathlib.Path(__file__).parents[2]))
import upload  # noqa
import base_utilities  # noqa
import pandas  # noqa
import json  # noqa
with base_utilities.change_directory():
    with open("../aws_creds.json") as f:
        aws_creds = json.load(f)
df1 = pandas.DataFrame([{"a": "hi"}, {"a": "hi"}])
df2 = pandas.DataFrame([{"a": "hi" * 100}, {"a": "hi"}])


def test_column_expansion():
    upload.upload(
        source=df1,
        schema_name="sb_pm",
        table_name="unit_test_column_expansion",
        upload_options={"drop_table": True},
        aws_info=aws_creds
    )
    exit()
    upload.upload(
        source=df2,
        schema_name="sb_pm",
        table_name="unit_test_column_expansion",
        upload_options={"drop_table": False},
        aws_info=aws_creds,
    )
test_column_expansion()
