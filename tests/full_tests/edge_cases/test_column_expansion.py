import pytest
from redshift_upload import upload, testing_utilities
import pandas

table_name = (
    "unit_" + __file__.replace("\\", "/").split("/")[-1].split(".")[0]
)  # we would just use __name__, but we don't want to run into __main__ if called directly


@pytest.fixture(autouse=True)
def setup_and_teardown():
    testing_utilities.drop_tables(table_name)
    yield  # this pauses the function for the tests to runa
    testing_utilities.drop_tables(table_name)


df1 = pandas.DataFrame([{"a": "hi"}, {"a": "hi"}])
df2 = pandas.DataFrame([{"a": "hi" * 100}, {"a": "hi"}])


def test_column_expansion(schema):
    upload(
        source=df1,
        schema_name=schema,
        table_name=table_name,
        upload_options={"drop_table": True},
    )
    upload(
        source=df2,
        schema_name=schema,
        table_name=table_name,
    )


if __name__ == "__main__":
    test_column_expansion()
