import sys
import pathlib
sys.path.insert(0, str(pathlib.Path(__file__).parents[2]) + '/redshift_upload')
import local_utilities
import pytest  # noqa


good_credentials = {
    "redshift_username": "",
    "redshift_password": "",
    "access_key": "",
    "secret_key": "",
    "bucket": '',
    "host": "",
    "dbname": "",
    "port": '',
}
bad_credentials = {
    "redshift_password": "",
    "access_key": "",
    "secret_key": "",
    "bucket": '',
    "host": "",
    "dbname": "",
    "port": '',
}
good_upload_options = {
}
bad_upload_options = {
    "truncate_table": True,
    "drop_table": True,
}


@pytest.mark.parametrize(
    "schema_name,table_name,upload_options,aws_info,is_good",
    [
        ("a", "b", good_upload_options, good_credentials, True),
        ("a", "b", good_upload_options, bad_credentials, False),
        ("a", "b", bad_upload_options, good_credentials, False),
        ("a", "b", bad_upload_options, bad_credentials, False),
        ("a", "", good_upload_options, good_credentials, False),
        ("a", None, good_upload_options, good_credentials, False),

    ],
)
def test_check_coherence(schema_name, table_name, upload_options, aws_info, is_good):
    if is_good:
        local_utilities.check_coherence(schema_name, table_name, upload_options, aws_info)
    else:
        with pytest.raises(ValueError):
            local_utilities.check_coherence(schema_name, table_name, upload_options, aws_info)
