from redshift_upload import credential_store  # noqa
import pytest
import os
from jsonschema.exceptions import ValidationError
import copy

sample_creds = {
    "db": {
        "host": "cluster.redshift.amazonaws.com",
        "port": 5439,
        "dbname": "test",
        "user": "user",
        "password": "pass",
    },
    "constants": {"bucket": "bucket-name", "default_schema": "test"},
    "s3": {
        "access_key": "AAAAAAAAAA0000000000",
        "secret_key": "AAAAAAAAAAAbbbbbbbbb999999999999999999/=",
    },
}


def create_user(user):
    creds = copy.deepcopy(sample_creds)
    creds["db"]["user"] = user
    return creds


@pytest.fixture(autouse=True)
def update_credential_store():  # overrides the other credential stores since we'll be destroying this one a lot
    credential_store.set_store("cred_store")
    yield
    credential_store.credentials.delete()


def test_path_fix():
    assert credential_store.credentials.file_path.endswith(".json")


def test_bad_creds():
    with pytest.raises(ValidationError):
        credential_store.credentials.add({1: 2})


def test_add_users():
    user_a = create_user("a")
    user_b = create_user("b")

    credential_store.credentials.add(user_a)
    assert credential_store.credentials.default == "a"
    assert len(credential_store.credentials.profiles) == 1
    credential_store.credentials.add(user_b)
    assert credential_store.credentials.default == "a"
    assert len(credential_store.credentials.profiles) == 2


def test_delete_users():
    for user in ("a", "b", "c"):
        credential_store.credentials.add(create_user(user))

    del credential_store.credentials["a"]
    assert credential_store.credentials.default == "b"
    assert len(credential_store.credentials.profiles) == 2

    del credential_store.credentials["c"]
    assert credential_store.credentials.default == "b"
    assert len(credential_store.credentials.profiles) == 1

    del credential_store.credentials["b"]
    assert credential_store.credentials.default is None
    assert len(credential_store.credentials.profiles) == 0


def test_get_default_user():
    credential_store.credentials.add(sample_creds)
    assert credential_store.credentials() == sample_creds
    del credential_store.credentials["user"]
    with pytest.raises(KeyError):
        assert credential_store.credentials()


def test_clear_store():
    credential_store.credentials.add(sample_creds)
    assert credential_store.credentials
    credential_store.credentials.clear()
    assert not credential_store.credentials


def test_delete_store():
    credential_store.credentials.delete()
    file_path = os.path.join(
        os.path.dirname(os.path.abspath(credential_store.__file__)),
        credential_store.credentials.file_path,
    )
    assert not os.path.exists(file_path)


if __name__ == "__main__":
    test_path_fix()
