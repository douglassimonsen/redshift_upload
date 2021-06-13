import sys
import pathlib
sys.path.insert(0, str(pathlib.Path(__file__).parents[2]))
from redshift_upload import local_utilities  # noqa
from redshift_upload.db_interfaces import dummy  # noqa
import pandas  # noqa
import pytest  # noqa

int1_in = [{"a": 1}, {"a": 2}, {"a": 3}]
int1_out = [{"a": '1'}, {"a": '2'}, {"a": '3'}]
int2_in = [{"a": 1.0}, {"a": "2"}, {"a": "3.0"}]
int2_out = [{"a": '1.0'}, {"a": "2"}, {"a": "3.0"}]

bool1_in = [{"a": "true"}, {"a": "true"}, {"a": "true"}]
bool1_out = [{"a": "True"}, {"a": "True"}, {"a": "True"}]
bool2_in = [{"a": True}, {"a": False}, {"a": "True"}]
bool2_out = [{"a": "True"}, {"a": "False"}, {"a": "True"}]
bool3_in = [{"a": True}, {"a": False}, {"a": None}]
bool3_out = [{"a": "True"}, {"a": "False"}, {"a": ""}]

dt1_in = [{"a": "2020-01-01 00:00:02"}, {"a": "2020-01-01 00:00:01"}, {"a": None}]
dt1_out = [{"a": "2020-01-01 00:00:00"}, {"a": "2020-01-01 00:00:01"}, {"a": ""}]


float1_in = [{"a": "1"}, {"a": 2.1}, {"a": None}]
float1_out = [{"a": 1.0}, {"a": 2.1}, {"a": None}]


@pytest.mark.parametrize(
    "df_in,df_out,typ",
    [
        (int1_in, int1_in, "SMALLINT"),
        (int2_in, int2_in, "SMALLINT"),
        (bool1_in, bool1_in, "BOOLEAN"),
        (bool2_in, bool2_in, "BOOLEAN"),
        (bool3_in, bool3_in, "BOOLEAN"),
        (dt1_in, dt1_in, "TIMESTAMP"),
        (float1_in, float1_out, "DOUBLE PRECISION"),
    ],
)
def test_forcible_conversion(df_in, df_out, typ):
    df_in = local_utilities.load_source(df_in)
    df_in.predefined_columns = {}
    local_utilities.fix_column_types(df_in, dummy.Interface(), False)
    assert df_in.column_types['a']['type'] == typ
    # assert str(list(df_in.dictrows())) == str(df_out)

if __name__ == '__main__':
    test_forcible_conversion(int1_in, int1_out, "SMALLINT")
