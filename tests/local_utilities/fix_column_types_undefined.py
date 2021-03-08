import sys
import pathlib
sys.path.insert(0, str(pathlib.Path(__file__).parents[2]))
from redshift_upload import local_utilities  # noqa
from redshift_upload.db_interfaces import dummy  # noqa
import pandas  # noqa
import pytest  # noqa

int1_in = pandas.DataFrame([{"a": 1}, {"a": 2}, {"a": 3}])
int1_out = pandas.DataFrame([{"a": 1}, {"a": 2}, {"a": 3}])
int2_in = pandas.DataFrame([{"a": 1.0}, {"a": "2"}, {"a": "3.0"}])
int2_out = pandas.DataFrame([{"a": 1}, {"a": 2}, {"a": 3}])

bool1_in = pandas.DataFrame([{"a": "true"}, {"a": "true"}, {"a": "true"}])
bool1_out = pandas.DataFrame([{"a": "True"}, {"a": "True"}, {"a": "True"}])
bool2_in = pandas.DataFrame([{"a": True}, {"a": False}, {"a": "true"}])
bool2_out = pandas.DataFrame([{"a": "True"}, {"a": "False"}, {"a": "True"}])
bool3_in = pandas.DataFrame([{"a": True}, {"a": False}, {"a": "nan"}])
bool3_out = pandas.DataFrame([{"a": "True"}, {"a": "False"}, {"a": ""}])

dt1_in = pandas.DataFrame([{"a": "2020-01-01"}, {"a": "2020-01-01 00:00:01.01"}, {"a": None}])
dt1_out = pandas.DataFrame([{"a": "2020-01-01 00:00:00.000000"}, {"a": "2020-01-01 00:00:01.010000"}, {"a": ""}])


float1_in = pandas.DataFrame([{"a": "1"}, {"a": 2.1}, {"a": None}])
float1_out = pandas.DataFrame([{"a": 1.0}, {"a": 2.1}, {"a": None}])


@pytest.mark.parametrize(
    "df_in,df_out,typ",
    [
        (int1_in, int1_out, "bigint"),
        (int2_in, int2_out, "bigint"),
        (bool1_in, bool1_out, "boolean"),
        (bool2_in, bool2_out, "boolean"),
        (bool3_in, bool3_out, "boolean"),
        (dt1_in, dt1_out, "timestamp"),
        (float1_in, float1_out, "double precision"),
    ],
)
def test_forcible_conversion(df_in, df_out, typ):
    act_df_out, types_out = local_utilities.fix_column_types(df_in, {}, dummy.Interface(), False)
    assert types_out == {"a": typ}
    assert act_df_out.equals(df_out) or (act_df_out == df_out).all().iat[0]


if __name__ == '__main__':
    test_forcible_conversion(int1_in, int1_out, "bigint")
