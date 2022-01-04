import sys
import pathlib
sys.path.insert(0, str(pathlib.Path(__file__).parents[2]))
from redshift_upload import upload, base_utilities, testing_utilities  # noqa
import pandas  # noqa
import json  # noqa
import pytest
with base_utilities.change_directory():
    with open("../aws_creds.json") as f:
        aws_creds = json.load(f)


@pytest.fixture
def fix():
    testing_utilities.drop_table("unit_test_add_column")


df1 = pandas.DataFrame([{"a": "hi"}, {"a": "hi"}] * 10)
df2 = pandas.DataFrame([{"a": "hi"}, {"a": "hi", "b": pandas.Timestamp('2021-01-01'), "c": 3}] * 10,)


def test_add_column():
    upload(
        source=df1,
        schema_name="public",
        table_name="unit_test_add_column",
        upload_options={'load_in_parallel': 10, "drop_table": True},
        aws_info=aws_creds
    )
    with pytest.raises(NotImplementedError):
        upload(
            source=df2,
            schema_name="public",
            table_name="unit_test_add_column",
            aws_info=aws_creds
        )


if __name__ == '__main__':
    test_add_column()
