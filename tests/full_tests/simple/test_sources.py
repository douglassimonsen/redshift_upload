import pytest
from redshift_upload import upload, base_utilities, testing_utilities  # noqa
import pandas  # noqa

table_name = (
    "unit_" + __file__.replace("\\", "/").split("/")[-1].split(".")[0]
)  # we would just use __name__, but we don't want to run into __main__ if called directly


@pytest.fixture(autouse=True)
def setup_and_teardown():
    testing_utilities.drop_tables(table_name)
    yield  # this pauses the function for the tests to runa
    testing_utilities.drop_tables(table_name)


def test_load_from_df(schema):
    upload(
        source=pandas.DataFrame([{"a": "hi"}, {"a": "hi"}]),
        schema_name=schema,
        table_name=table_name,
    )


def test_load_from_list(schema):
    upload(
        source=[{"a": "hi"}, {"a": "hi"}],
        schema_name=schema,
        table_name=table_name,
    )


def test_load_from_bytes(schema):
    upload(
        source=b"a\nb\nc\n",
        schema_name=schema,
        table_name=table_name,
    )


def test_load_from_string(schema):
    upload(
        source="a\nb\nc\n",
        schema_name=schema,
        table_name=table_name,
    )


def test_load_from_file(schema):
    with base_utilities.change_directory():
        upload(
            source="../load_source.csv",
            schema_name=schema,
            table_name=table_name,
        )


if __name__ == "__main__":
    test_load_from_string()
