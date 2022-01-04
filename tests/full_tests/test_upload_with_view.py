import sys
import pathlib
sys.path.insert(0, str(pathlib.Path(__file__).parents[2]))
from redshift_upload import upload, base_utilities, testing_utilities  # noqa
import pandas  # noqa
import json  # noqa
import datetime  # noqa
import pytest  # noqa
with base_utilities.change_directory():
    with open("../aws_creds.json") as f:
        aws_creds = json.load(f)
table_name = "unit_" + __file__.replace('\\', '/').split('/')[-1].split('.')[0]  # we would just use __name__, but we don't want to run into __main__ if called directly


@pytest.fixture(autouse=True)
def setup_and_teardown():
    testing_utilities.drop_tables(table_name)
    yield  # this pauses the function for the tests to runa
    testing_utilities.drop_tables(table_name)


today = datetime.datetime.today()
today_date = today.date()
df_int = [{"a": 1}, {"a": 2}, {"a": 3}]
df_float = [{"a": 1.0}, {"a": 2.1}, {"a": 3.0}]
df_dt = [{"a": today.strftime("%Y-%m-%d %H:%M:%S")}, {"a": today.strftime("%Y-%m-%d %H:%M:%S")}, {"a": None}]
df_date = [{"a": today_date}, {"a": today_date}, {"a": None}]
df_bool = [{"a": False}, {"a": True}, {"a": None}]
df_text = [{"a": "Goodbye"}, {"a": "hello"}, {"a": None}]


def dt_stringer(df):
    return df.astype(str).replace({'NaT': None})


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
def test_upload_with_view(df, formatter):
    interface = upload(
        source=df.copy(),  # needed for the comparison later
        schema_name="public",
        table_name=table_name,
        upload_options={"drop_table": True, "close_on_end": False},
        aws_info=aws_creds
    )

    with interface.get_db_conn() as conn:
        cursor = conn.cursor()
        cursor.execute(f"create or replace view {interface.schema_name}.{interface.table_name}_view as select * from {interface.schema_name}.{interface.table_name}")
        cursor.execute(f"create or replace view {interface.schema_name}.{interface.table_name}_view2 as select * from {interface.schema_name}.{interface.table_name}_view")
        conn.commit()
    interface = upload(
        source=df.copy(),  # needed for the comparison later
        schema_name="public",
        table_name=table_name,
        upload_options={"drop_table": True, "close_on_end": False},
        aws_info=aws_creds
    )

    with interface.get_db_conn() as conn:
        df_out = pandas.read_sql(f"select * from {interface.schema_name}.{interface.table_name}_view2 order by a", conn)
    if formatter is not None:
        df_out = formatter(df_out)
    df_out = df_out.to_dict("records")
    assert str(df) == str(df_out)


if __name__ == '__main__':
    test_upload_with_view(df_dt, dt_stringer)
