from redshift_upload import local_utilities  # noqa
import pytest  # noqa


good_credentials = {
    "db": {
        "user": "dummy",
        "password": "dummy",
        "host": "dummy",
        "dbname": "dummy",
        "port": "dummy",
    },
    "s3": {
        "access_key": "dummy",
        "secret_key": "dummy",
    },
    "constants": {
        "bucket": "dummy",
    },
}
bad_credentials = {
    "db": {
        "user": "",
        "password": "",
        "host": "",
        "dbname": "",
        "port": "",
    },
    "s3": {
        "access_key": "",
        "secret_key": "",
    },
    "constants": {
        "bucket": "",
    },
}
good_upload_options = {}
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
        local_utilities.check_coherence(
            schema_name, table_name, upload_options, aws_info
        )
    else:
        with pytest.raises(ValueError):
            local_utilities.check_coherence(
                schema_name, table_name, upload_options, aws_info
            )


if __name__ == "__main__":
    test_check_coherence("a", "b", good_upload_options, good_credentials, True)
