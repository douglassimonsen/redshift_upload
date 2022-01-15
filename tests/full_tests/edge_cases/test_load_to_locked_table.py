import datetime
import psycopg2
import pytest
import psycopg2.errors
from redshift_upload import upload, testing_utilities  # noqa

table_name = (
    "unit_" + __file__.replace("\\", "/").split("/")[-1].split(".")[0]
)  # we would just use __name__, but we don't want to run into __main__ if called directly

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
    )
    interface.get_exclusive_lock()
    with pytest.raises(
        psycopg2.errors.QueryCanceled
    ):  # this test can be flaky when run with all the rest. I think Redshift removes the lock when there's a lot of activity
        upload(
            source=df,  # needed for the comparison later
            schema_name=schema,
            table_name="unit_test_simple_upload_incompatible_types",
        )
    interface
    # I'm hoping this will pause the GC. The issue is the connection is being closed
    # and the exclusive lock removed before the second upload can run into it


if __name__ == "__main__":
    test_drop_table("public")
