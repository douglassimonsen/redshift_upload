import sys
import pathlib
sys.path.insert(0, str(pathlib.Path(__file__).parents[2]))
from redshift_upload import local_utilities  # noqa
from redshift_upload.db_interfaces import dummy  # noqa
import pandas  # noqa
import pytest  # noqa

int_in = pandas.DataFrame([{"a": 10000}, {"a": 20000}, {"a": 30000}])
int_out1 = pandas.DataFrame([{"a": "10000"}, {"a": "20000"}, {"a": "30000"}])
int_out2 = pandas.DataFrame([{"a": 10000.0}, {"a": 20000.0}, {"a": 30000.0}])
int_out3 = pandas.DataFrame([{"a": "1970-01-01 00:00:00.000010"}, {"a": "1970-01-01 00:00:00.000020"}, {"a": "1970-01-01 00:00:00.000030"}])
int_out4 = pandas.DataFrame([{"a": 10000}, {"a": 20000}, {"a": 30000}])

bool_in = pandas.DataFrame([{"a": "True"}, {"a": False}, {"a": None}])
bool_out1 = pandas.DataFrame([{"a": "True"}, {"a": "False"}, {"a": None}])
bool_out2 = pandas.DataFrame([{"a": True}, {"a": False}, {"a": None}])


@pytest.mark.parametrize(
    "df_in,df_out,typ",
    [
        (int_in.copy(), int_out1, "varchar(10)"),
        (int_in.copy(), int_out2, "double precision"),
        (int_in.copy(), int_out3, "timestamp"),
        (int_in.copy(), int_out4, "bigint"),
        (bool_in.copy(), bool_out1, "varchar(10)"),
        (bool_in.copy(), bool_out2, "boolean"),
    ],
)
def test_forcible_conversion_type_defined(df_in, df_out, typ):
    act_df_out, types_out = local_utilities.fix_column_types(df_in, {"a": {"type": typ}}, dummy.Interface(), False)
    assert types_out == {"a": typ}
    assert act_df_out.to_csv(None) == df_out.to_csv(None)  # we do this because ultimately, it's the serialization that matters


if __name__ == '__main__':
    test_forcible_conversion_type_defined(int_in.copy(), int_out1, "varchar(10)")
