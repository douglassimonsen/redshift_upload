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
df_int = pandas.DataFrame([{"a": 1}, {"a": 2}, {"a": 3}])
df_float = pandas.DataFrame([{"a": 1.0}, {"a": 2.1}, {"a": 3.0}])
df_dt = pandas.DataFrame([{"a": today}, {"a": today}, {"a": None}])
df_date = pandas.DataFrame([{"a": today_date}, {"a": today_date}, {"a": None}])
df_bool = pandas.DataFrame([{"a": True}, {"a": False}, {"a": None}])
df_text = pandas.DataFrame([{"a": "hello"}, {"a": "Goodbye"}, {"a": None}])


@pytest.mark.parametrize(
    "df",
    [
        df_int,
        df_float,
        df_dt,
        df_date,
        df_bool,
        df_text,
    ],
)
def test_drop_table(df):
    df = df.copy()
    df["order_col"] = df.index
    interface = upload(
        source=df.copy(),  # needed for the comparison later
        schema_name="sb_pm",
        table_name="unit_test_simple_upload_drop_table",
        upload_options={"drop_table": True},
        aws_info=aws_creds
    )
    with interface.get_db_conn() as conn:
        df_out = pandas.read_sql(f"select * from {interface.full_table_name} order by order_col", conn)
    assert df.equals(df_out) or (df == df_out).all().iat[0]


if __name__ == '__main__':
    test_drop_table(df_int)
