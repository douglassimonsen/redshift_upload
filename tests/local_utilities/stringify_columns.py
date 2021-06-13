import sys
import pathlib
sys.path.insert(0, str(pathlib.Path(__file__).parents[2]))
from redshift_upload import local_utilities  # noqa
from redshift_upload.db_interfaces import dummy  # noqa
import pandas  # noqa
import pytest  # noqa

df_in = pandas.DataFrame([{'a': 1, 1: 1123123123142}, {'a': 2, 1: 2, 3: 123456}])


@pytest.mark.parametrize(
    "df_in",
    [
        df_in
    ],
)
def test_stringify_columns(df_in):
    df_in = local_utilities.load_source(df_in)
    df_in.predefined_columns = {}
    local_utilities.fix_column_types(df_in, dummy.Interface(), False)
    assert tuple(x['type'] for x in df_in.column_types.values()) == ('SMALLINT', 'BIGINT', 'INTEGER')


if __name__ == '__main__':  
    test_stringify_columns(df_in)