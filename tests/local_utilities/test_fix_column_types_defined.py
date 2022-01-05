from redshift_upload import local_utilities  # noqa
from redshift_upload.db_interfaces import dummy  # noqa
import pytest  # noqa

int_in = [{"a": 10000}, {"a": 20000}, {"a": 30000}]
int_out1 = [{"a": "10000"}, {"a": "20000"}, {"a": "30000"}]
int_out2 = [{"a": 10000.0}, {"a": 20000.0}, {"a": 30000.0}]
dt_in = [
    {"a": "1970-01-01 00:00:00"},
    {"a": "1970-01-01 00:00:00"},
    {"a": "1970-01-01 00:00:00"},
]
int_out4 = [{"a": 10000}, {"a": 20000}, {"a": 30000}]

bool_in = [{"a": "True"}, {"a": False}, {"a": None}]
bool_out1 = [{"a": "True"}, {"a": "False"}, {"a": None}]
bool_out2 = [{"a": True}, {"a": False}, {"a": None}]


@pytest.mark.parametrize(
    "df_in,df_out,typ",
    [
        (int_in, int_out1, "VARCHAR"),
        (int_in, int_out2, "DOUBLE PRECISION"),
        (dt_in, dt_in, "TIMESTAMP"),
        (int_in, int_out4, "BIGINT"),
        (bool_in, bool_out1, "VARCHAR"),
        (bool_in, bool_out2, "BOOLEAN"),
    ],
)
def test_forcible_conversion_type_defined(df_in, df_out, typ):
    df_in = local_utilities.load_source(df_in)
    df_in.predefined_columns = {"a": {"type": typ, "suffix": None}}
    local_utilities.fix_column_types(df_in, dummy.Interface(), False)
    assert df_in.column_types["a"]["type"] == typ
    # assert act_df_out.to_csv(None) == df_out.to_csv(None)  # we do this because ultimately, it's the serialization that matters


if __name__ == "__main__":
    test_forcible_conversion_type_defined(int_in, int_out1, "VARCHAR")
