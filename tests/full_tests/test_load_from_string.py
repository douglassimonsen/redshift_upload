import pytest
import sys
import pathlib
sys.path.insert(0, str(pathlib.Path(__file__).parents[2]))
from redshift_upload import upload, base_utilities, testing_utilities  # noqa
import pandas  # noqa
import json  # noqa
with base_utilities.change_directory():
    with open("../aws_creds.json") as f:
        aws_creds = json.load(f)
table_name = "unit_" + __file__.replace('\\', '/').split('/')[-1].split('.')[0]  # we would just use __name__, but we don't want to run into __main__ if called directly


@pytest.fixture(autouse=True)
def setup_and_teardown():
    testing_utilities.drop_tables(table_name)
    yield  # this pauses the function for the tests to runa
    testing_utilities.drop_tables(table_name)


def test_load_from_string():
    upload(
        source=pandas.DataFrame([{"a": "hi"}, {"a": "hi"}]),
        schema_name="public",
        table_name=table_name,
        upload_options={'load_in_parallel': 2},
        aws_info=aws_creds,
        # log_level="WARNING"
    )
    upload(
        source="a\nb\nc\n",
        schema_name="public",
        table_name=table_name,
        upload_options={'load_in_parallel': 2},
        aws_info=aws_creds,
        # log_level="WARNING"
    )
    with base_utilities.change_directory():
        upload(
            source="load_source.csv",
            schema_name="public",
            table_name=table_name,
            upload_options={'load_in_parallel': 2},
            aws_info=aws_creds,
            # log_level="WARNING"
        )


if __name__ == '__main__':
    test_load_from_string()
