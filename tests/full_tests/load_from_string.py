import sys
import pathlib
sys.path.insert(0, str(pathlib.Path(__file__).parents[2]))
from redshift_upload import upload, base_utilities  # noqa
import pandas  # noqa
import json  # noqa
with base_utilities.change_directory():
    with open("../aws_creds.json") as f:
        aws_creds = json.load(f)


def test_load_from_string():
    upload(
        source=pandas.DataFrame([{"a": "hi"}, {"a": "hi"}]),
        schema_name="sb_pm",
        table_name="unit_test_column_expansion",
        upload_options={"load_as_csv": False, 'load_in_parallel': 2},
        aws_info=aws_creds,
        # log_level="WARNING"
    )
    upload(
        source="a\nb\nc\n",
        schema_name="sb_pm",
        table_name="unit_test_column_expansion",
        upload_options={"load_as_csv": True, 'load_in_parallel': 2},
        aws_info=aws_creds,
        # log_level="WARNING"
    )
    with base_utilities.change_directory():
        upload(
            source="load_source.csv",
            schema_name="sb_pm",
            table_name="unit_test_column_expansion",
            upload_options={"load_as_csv": True, 'load_in_parallel': 2},
            aws_info=aws_creds,
            # log_level="WARNING"
        )


if __name__ == '__main__':
    test_load_from_string()
