import sys
import pathlib

sys.path.insert(0, str(pathlib.Path(__file__).parents[2]))
from redshift_upload import upload, base_utilities, testing_utilities  # noqa
import pandas  # noqa
import json  # noqa
import pytest

with base_utilities.change_directory():
    with open("../aws_creds.json") as f:
        aws_creds = json.load(f)
table_name = (
    "unit_" + __file__.replace("\\", "/").split("/")[-1].split(".")[0]
)  # we would just use __name__, but we don't want to run into __main__ if called directly


@pytest.fixture(autouse=True)
def setup_and_teardown():
    testing_utilities.drop_tables(table_name)
    yield  # this pauses the function for the tests to run
    testing_utilities.drop_tables(table_name)


df1 = pandas.DataFrame([{"a": "hi"}, {"a": "hi"}] * 10)
df2 = pandas.DataFrame(
    [{"a": "hi"}, {"a": "hi", "b": pandas.Timestamp("2021-01-01"), "c": 3}] * 10,
)


def test_add_column(schema):
    upload(
        source=df1,
        schema_name=schema,
        table_name=table_name,
        upload_options={"load_in_parallel": 10, "drop_table": True},
        aws_info=aws_creds,
    )
    with pytest.raises(NotImplementedError):
        upload(
            source=df2, schema_name=schema, table_name=table_name, aws_info=aws_creds
        )


if __name__ == "__main__":
    test_add_column()
