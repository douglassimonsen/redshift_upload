import sys
import pathlib
import pandas
sys.path.insert(0, str(pathlib.Path(__file__).parents[2]))
from redshift_upload import local_utilities, base_utilities  # noqa
import pytest  # noqa

raw_df = [{'1': 'a', '2': 2}]
df = pandas.DataFrame(raw_df)


@pytest.mark.parametrize(
    "source,output",
    [
        (df, df),
        (raw_df, df),
        ("load_source.csv", df),
    ],
)
def test_load_source(source, output):
    with base_utilities.change_directory():
        assert local_utilities.load_source(source, [], {}, {'load_as_csv': False}).equals(output)


if __name__ == '__main__':
    test_load_source(df, df)
