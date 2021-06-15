import sys
import pathlib
sys.path.insert(0, str(pathlib.Path(__file__).parents[2]))
from redshift_upload import upload, base_utilities  # noqa
import pandas  # noqa
import json  # noqa
with base_utilities.change_directory():
    with open("../aws_creds.json") as f:
        aws_creds = json.load(f)


def test_load_on_disk():
    with base_utilities.change_directory():
        upload(
            source="load_source.csv",
            schema_name="public",
            table_name="unit_test_column_expansion",
            upload_options={'on_disk': True},
            aws_info=aws_creds,
            # log_level="WARNING"
        )


if __name__ == '__main__':
    test_load_on_disk()
