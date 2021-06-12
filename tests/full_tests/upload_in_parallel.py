import sys
import pathlib
sys.path.insert(0, str(pathlib.Path(__file__).parents[2]))
from redshift_upload import upload, base_utilities  # noqa
import pandas  # noqa
import json  # noqa
with base_utilities.change_directory():
    with open("../aws_creds.json") as f:
        aws_creds = json.load(f)
df = pandas.DataFrame([{"a": "hi"}, {"a": "hi"}] * 100)


def test_upload_in_parallel():
    upload(
        source=df,
        schema_name="public",
        table_name="unit_test_column_expansion",
        upload_options={'load_in_parallel': 10},
        aws_info=aws_creds
    )


if __name__ == '__main__':
    test_upload_in_parallel()
