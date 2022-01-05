import pytest
from redshift_upload import upload, testing_utilities  # noqa
import datetime  # noqa

table_name = (
    "unit_" + __file__.replace("\\", "/").split("/")[-1].split(".")[0]
)  # we would just use __name__, but we don't want to run into __main__ if called directly


@pytest.fixture(autouse=True)
def setup_and_teardown():
    testing_utilities.drop_tables(table_name)
    yield  # this pauses the function for the tests to runa
    testing_utilities.drop_tables(table_name)


today = datetime.datetime.today()
today_date = today.date()
df1 = [{"a" * 50: 1}]
df2 = [{"a" * 50: 1}]


def test_drop_table(schema):
    upload(
        source=df1,  # needed for the comparison later
        schema_name=schema,
        table_name=table_name,
        upload_options={"drop_table": True},
    )
    upload(
        source=df2,  # needed for the comparison later
        schema_name=schema,
        table_name=table_name,
    )


if __name__ == "__main__":
    test_drop_table()
