import sys
import pathlib
import pytest
sys.path.insert(0, str(pathlib.Path(__file__).parents[2]))
from redshift_upload import upload, base_utilities, testing_utilities  # noqa
import json  # noqa
import datetime  # noqa
with base_utilities.change_directory():
    with open("../aws_creds.json") as f:
        aws_creds = json.load(f)
table_name = "unit_" + __file__.replace('\\', '/').split('/')[-1].split('.')[0]  # we would just use __name__, but we don't want to run into __main__ if called directly


@pytest.fixture(autouse=True)
def setup_and_teardown():
    testing_utilities.drop_tables(table_name)
    yield  # this pauses the function for the tests to runa
    testing_utilities.drop_tables(table_name)


today = datetime.datetime.today()
today_date = today.date()
df1 = [{"a": 1, "b": "hi"}, {"a": 2}, {"a": 3}]
df2 = [{"a": 1}, {"a": 2}, {"a": 3}]


def test_drop_table(schema):
    upload(
        source=df1,  # needed for the comparison later
        schema_name=schema,
        table_name=table_name,
        upload_options={"drop_table": True},
        aws_info=aws_creds
    )
    upload(
        source=df2,  # needed for the comparison later
        schema_name=schema,
        table_name=table_name,
        aws_info=aws_creds
    )


if __name__ == '__main__':
    test_drop_table()
