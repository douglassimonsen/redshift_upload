import sys
import pathlib
sys.path.insert(0, str(pathlib.Path(__file__).parents[2]))
from redshift_upload import upload, base_utilities  # noqa
import pandas  # noqa
import json  # noqa
import datetime  # noqa
import pytest  # noqa
with base_utilities.change_directory():
    with open("../aws_creds.json") as f:
        aws_creds = json.load(f)
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
def test_truncate_table(df, formatter):
    upload(
        source=df,  # needed for the comparison later
        schema_name="public",
        table_name="unit_test_simple_upload_truncate_table",
        upload_options={"drop_table": True},
        aws_info=aws_creds
    )
    interface = upload(
        source=df,  # needed for the comparison later
        schema_name="public",
        table_name="unit_test_simple_upload_truncate_table",
        upload_options={"truncate_table": True, "close_on_end": False},
        aws_info=aws_creds
    )
    with interface.get_db_conn() as conn:
        df_out = pandas.read_sql(f"select * from {interface.full_table_name} order by a", conn)
    if formatter is not None:
        df_out = formatter(df_out)
    df_out = df_out.to_dict("records")
    assert str(df) == str(df_out)


if __name__ == '__main__':
    test_truncate_table(df_int, None)
