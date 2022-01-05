import sys
import pathlib

sys.path.insert(0, str(pathlib.Path(__file__).parents[2]))
from redshift_upload import upload, testing_utilities  # noqa
import pandas  # noqa
import datetime  # noqa
import pytest  # noqa

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
df_int = [{"a": 1}, {"a": 2}, {"a": 3}]
df_float = [{"a": 1.0}, {"a": 2.1}, {"a": 3.0}]
df_dt = [
    {"a": today.strftime("%Y-%m-%d %H:%M:%S")},
    {"a": today.strftime("%Y-%m-%d %H:%M:%S")},
    {"a": None},
]
df_date = [{"a": today_date}, {"a": today_date}, {"a": None}]
df_bool = [{"a": False}, {"a": True}, {"a": None}]
df_text = [{"a": "Goodbye"}, {"a": "hello"}, {"a": None}]


def dt_stringer(df):
    return df.astype(str).replace({"NaT": None})


@pytest.mark.parametrize(
    "df,formatter",
    [
        (df_int, None),
        (df_float, None),
        (df_dt, dt_stringer),
        (df_date, None),
        (df_bool, None),
        (df_text, None),
    ],
)
def test_drop_table(df, formatter, schema):
    interface = upload(
        source=df.copy(),  # needed for the comparison later
        schema_name=schema,
        table_name=table_name,
        upload_options={"drop_table": True, "close_on_end": False},
    )
    with interface.get_db_conn() as conn:
        df_out = pandas.read_sql(
            f"select * from {interface.full_table_name} order by a", conn
        )
    if formatter is not None:
        df_out = formatter(df_out)
    df_out = df_out.to_dict("records")
    assert str(df) == str(df_out)


if __name__ == "__main__":
    test_drop_table(df_int, None)
