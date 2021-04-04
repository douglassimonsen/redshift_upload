import sys
import pathlib
sys.path.insert(0, str(pathlib.Path(__file__).parents[2]))
from redshift_upload import local_utilities  # noqa
from redshift_upload.db_interfaces import dummy  # noqa
import pandas  # noqa
import pytest  # noqa

df_in = pandas.DataFrame([{'a': 1, 1: 1}, {'a': 2, 1: 2}])
df_out = pandas.DataFrame([{'a': 1, '1': 1}, {'a': 2, '1': 2}])


def test_stringify_columns():
    act_df_out, types_out = local_utilities.fix_column_types(df_in, {}, dummy.Interface(), False)
    print(act_df_out.columns)
    assert df_in.equals(df_out)


if __name__ == '__main__':  
    test_stringify_columns()