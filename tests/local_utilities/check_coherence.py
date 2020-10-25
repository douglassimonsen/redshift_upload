import sys
import pathlib
sys.path.insert(0, str(pathlib.Path(__file__).parents[2]))
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
    "upload_options,aws_info,is_good",
    [
        (good_upload_options, good_credentials, True),
        (good_upload_options, bad_credentials, False),
        (bad_upload_options, good_credentials, False),
        (bad_upload_options, bad_credentials, False),
    ],
)
def test_check_coherence(upload_options, aws_info, is_good):
    if is_good:
        local_utilities.check_coherence(upload_options, aws_info)
    else:
        with pytest.raises(ValueError):
            local_utilities.check_coherence(upload_options, aws_info)
