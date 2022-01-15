import pytest
from redshift_upload import upload, testing_utilities  # noqa
import pandas  # noqa

table_name = (
    "unit_" + __file__.replace("\\", "/").split("/")[-1].split(".")[0]
)  # we would just use __name__, but we don't want to run into __main__ if called directly


@pytest.fixture(autouse=True)
def setup_and_teardown():
    testing_utilities.drop_tables(table_name)
    yield  # this pauses the function for the tests to runa
    testing_utilities.drop_tables(table_name)


df = pandas.DataFrame([{"a": "2020-01-01 04:00"}, {"a": "2020-01-01 04:00"}] * 100)


def test_upload_types(schema):
    upload(
        source=df,
        schema_name=schema,
        table_name=table_name,
        upload_options={"drop_table": True},
    )


if __name__ == "__main__":
    test_upload_types()
