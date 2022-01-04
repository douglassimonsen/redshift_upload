import sys
import pathlib
import json
import datetime
import psycopg2
import pytest
import psycopg2.errors
sys.path.insert(0, str(pathlib.Path(__file__).parents[2]))
from redshift_upload import upload, base_utilities, testing_utilities  # noqa
with base_utilities.change_directory():
    with open("../aws_creds.json") as f:
        aws_creds = json.load(f)
table_name = "unit_" + __file__.replace('\\', '/').split('/')[-1].split('.')[0]  # we would just use __name__, but we don't want to run into __main__ if called directly

today = datetime.datetime.today()
today_date = today.date()
df = [{"a": 1}]


@pytest.fixture(autouse=True)
def setup_and_teardown():
    testing_utilities.drop_tables(table_name)
    yield  # this pauses the function for the tests to run
    testing_utilities.drop_tables(table_name)



def test_drop_table(schema):
    interface = upload(
        source=df,  # needed for the comparison later
        schema_name=schema,
        table_name="unit_test_simple_upload_incompatible_types",
        upload_options={"drop_table": True, "close_on_end": False},
        aws_info=aws_creds
    )
    interface.get_exclusive_lock()
    with pytest.raises(psycopg2.errors.QueryCanceled):
        upload(
            source=df,  # needed for the comparison later
            schema_name=schema,
            table_name="unit_test_simple_upload_incompatible_types",
            aws_info=aws_creds
    )


if __name__ == '__main__':
    test_drop_table('public')
