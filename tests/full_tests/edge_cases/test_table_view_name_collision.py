import pytest
from redshift_upload import upload, testing_utilities  # noqa

table_name = (
    "unit_" + __file__.replace("\\", "/").split("/")[-1].split(".")[0]
)  # we would just use __name__, but we don't want to run into __main__ if called directly


@pytest.fixture(autouse=True)
def setup_and_teardown():
    testing_utilities.drop_tables(table_name)
    yield  # this pauses the function for the tests to runa
    testing_utilities.drop_tables(table_name)


df = [{"a": 1}, {"a": 2}, {"a": 3}]


def test_drop_table(schema):
    interface = upload(
        source=df,  # needed for the comparison later
        schema_name=schema,
        table_name=table_name,
        upload_options={"drop_table": True, "close_on_end": False},
    )
    with interface.get_db_conn() as conn:
        conn.cursor().execute(
            f"create or replace view {table_name}2 as (select * from {table_name})"
        )
        conn.commit()
    with pytest.raises(ValueError):
        interface = upload(
            source=df,  # needed for the comparison later
            schema_name=schema,
            table_name=table_name + "2",
            upload_options={"drop_table": True},
        )


if __name__ == "__main__":
    test_drop_table()
