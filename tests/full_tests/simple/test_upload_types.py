import pytest
from redshift_upload import upload, testing_utilities  # noqa


table_name = (
    "unit_" + __file__.replace("\\", "/").split("/")[-1].split(".")[0]
)  # we would just use __name__, but we don't want to run into __main__ if called directly


@pytest.fixture(autouse=True)
def setup_and_teardown():
    testing_utilities.drop_tables(table_name)
    yield  # this pauses the function for the tests to runa
    # testing_utilities.drop_tables(table_name)


df = [
    {
        "dt_col": "2020-01-01 04:00:00",
        "int_col": "1",
        "time_col": "04:12:34",
        "var_col": "asd",
        "date_col": "2020-01-01",
        "float_col": "1.2",
    }
    for _ in range(100)
]


def test_upload_types(schema):
    interface = upload(
        source=df,
        schema_name=schema,
        table_name=table_name,
        upload_options={"close_on_end": False, "drop_table": True},
    )
    testing_utilities.compare_sources(
        df,
        f"{schema}.{table_name}",
        interface.get_db_conn(),
        field_types=[
            "timestamp without time zone",
            "smallint",
            "time without time zone",
            "character varying(3)",
            "date",
            "double precision",
        ],
    )


if __name__ == "__main__":
    test_upload_types()
