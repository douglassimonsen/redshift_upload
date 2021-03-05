import sys
import pathlib
sys.path.insert(0, str(pathlib.Path(__file__).parents[2]) + '/redshift_upload')
import upload  # noqa
import base_utilities  # noqa
import pandas  # noqa
import json  # noqa
with base_utilities.change_directory():
    with open("../aws_creds.json") as f:
        aws_creds = json.load(f)
df = pandas.DataFrame([{"a": "hi"}, {"a": "hi"}] * 100)


def test_column_expansion():
    upload.upload(
        source=df,
        schema_name="sb_pm",
        table_name="unit_test_column_expansion",
        upload_options={"drop_table": True, 'load_in_parallel': 10},
        aws_info=aws_creds
    )


test_column_expansion()
