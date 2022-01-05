from redshift_upload import upload, testing_utilities
import pandas
import pytest

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
    )
    with pytest.raises(NotImplementedError):
        upload(
            source=df2, schema_name=schema, table_name=table_name,
        )


if __name__ == "__main__":
    test_add_column('public')
