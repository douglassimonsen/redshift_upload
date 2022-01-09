try:
    from ..credential_store import credential_store
except ImportError:
    import os, sys

    sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    from credential_store import credential_store
import jsonschema
import psycopg2
import boto3
import random
import botocore.exceptions


params = [
    "host",
    "port",
    "dbname",
    "default_schema",
    "redshift_username",
    "redshift_password",
    "bucket",
    "access_key",
    "secret_key",
]
intro = """
Let's get you uploading! The library needs these credentials to function.
If you already have a default account set up in store.json, you can press
enter to accept the default for that param.
"""
default_user = credential_store.credentials.default
if default_user is not None:
    default_params = credential_store.credentials[default_user]
else:
    default_params = {}
s3_name = "library_test/" + "".join(
    random.choices([chr(65 + i) for i in range(26)], k=20)
)  # needs to be out here so repeated s3 checks don't create orphan objects


def get_val(param):
    question = f"What is the value for {param}"
    default_val = None

    if param in default_params:
        default_val = default_params[param]
        question += f" (default: {default_val})"

    question += ": "
    ret = input(question)
    if len(ret) == 0 and default_val is not None:
        return default_val
    if param == "port":
        ret = int(ret)
    return ret


def yes_no(question):
    raw = input(f"{question} (y/n): ").lower()
    if raw not in ("y", "n"):
        return yes_no(question)
    return raw


def fix_schema(user):
    print("Testing it matches the credential JSONSchema...")
    try:
        jsonschema.validate(user, credential_store.SCHEMA)
        return
    except jsonschema.exceptions.ValidationError as e:
        print(f"{e.path[0]}: {e.message}")
        user[e.path[0]] = get_val(e.path[0])
        fix_schema(user)
    print("Schema successfully validated!")


def test_s3(user):
    # test accesss/secret key
    # test bucket can be written to
    # test bucket can be deleted from
    s3 = boto3.resource(
        "s3",
        aws_access_key_id=user["access_key"],
        aws_secret_access_key=user["secret_key"],
    )
    obj = s3.Object(user["bucket"], s3_name)
    try:
        obj.put(Body=b"test")
    except botocore.exceptions.ClientError as e:
        if e.response["Error"]["Code"] == "InvalidAccessKeyId":
            print("It looks like the access key doesn't exist. Try another?")
            user["access_key"] = get_val("access_key")
            user["secret_key"] = get_val("secret_key")
            fix_schema(user)
            test_s3(user)

    obj.delete()
    exit()
    pass


def test_redshift(user):
    # test create/delete table
    pass


def test_connections(user):
    test_s3(user)
    test_redshift(user)


def test_vals(user):
    do_tests = "y"  # yes_no("Do you want to verify these values are correct?")
    if do_tests == "n":
        return
    fix_schema(user)
    print("Testing connections now")
    test_connections(user)
    print("Connections tested successfully")


user = {
    "host": "cluster-8htl2naazuk5.czdokwtvhved.us-east-1.redshift.amazonaws.com",
    "port": 5439,
    "dbname": "test",
    "default_schema": "public",
    "redshift_username": "admin",
    "redshift_password": "Password1",
    "bucket": "test-library-store-i36trvzczizz",
    "access_key": "AKIARTZ6RUF63QZK2WG4",
    "secret_key": "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",
}
test_vals(user)
exit()


def main():
    print(intro)
    user = {}
    for param in params:
        user[param] = get_val(param)
    print(user)
    test_vals(user)


if __name__ == "__main__":
    main()
