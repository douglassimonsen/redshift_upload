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
import botocore.errorfactory


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
table_name = "library_test_" + "".join(
    random.choices([chr(65 + i) for i in range(26)], k=20)
)  # needs to be out here so repeated redshift checks don't create orphan objects


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
        print("Schema successfully validated!")
        return
    except jsonschema.exceptions.ValidationError as e:
        print(f"{e.path[0]}: {e.message}")
        user[e.path[0]] = get_val(e.path[0])
        return fix_schema(user)


def unhandled_aws_error(error):
    print("Unhandled error :(")
    print(error)
    print(error.response)
    raise ValueError


def test_s3(user):
    # test accesss/secret key
    # test bucket can be written to
    # test bucket can be deleted from
    print("\tTesting S3 permissions")
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
            return test_s3(user)
        elif (
            e.response["Error"]["Code"] == "AccessDenied"
            and e.operation_name == "PutObject"
        ):
            print(
                "It looks like the access key doesn't have permission to write to the specified bucket. Try new access keys or bucket"
            )
            user["access_key"] = get_val("access_key")
            user["secret_key"] = get_val("secret_key")
            fix_schema(user)
            return test_s3(user)
        elif e.response["Error"]["Code"] == "NoSuchBucket":
            print("It looks like that bucket doesn't exist. Try another?")
            user["bucket"] = get_val("bucket")
            fix_schema(user)
            return test_s3(user)
        else:
            unhandled_aws_error(e)

    try:
        obj.delete()
    except botocore.exceptions.ClientError as e:
        if e.response["Error"]["Code"] == "AccessDenied":
            print(
                "It looks like the access key can't delete things. That's fine, the library will overwrite the blob with a blank body so it doesn't bloat costs"
            )
        else:
            unhandled_aws_error(e)

    print("\tS3 permissions tested successfully")


def test_redshift(user):
    # test create/delete table
    print("\tTesting Redshift Permissions")
    try:
        conn = psycopg2.connect(
            host=user["host"],
            dbname=user["dbname"],
            port=user["port"],
            user=user["redshift_username"],
            password=user["redshift_password"],
            connect_timeout=5,
        )
    except psycopg2.OperationalError as e:
        if "FATAL:  database" in e.args[0]:
            print("It looks like that database doesn't exist. Try entering another?")
            user["dbname"] = get_val("dbname")
            fix_schema(user)
            return test_redshift(user)
        elif "Unknown host" in e.args[0]:
            print("It looks like that host doesn't exist. Try entering another?")
            user["host"] = get_val("host")
            fix_schema(user)
            return test_redshift(user)
        elif "timeout expired" in e.args[0]:
            print(
                "The connection timed out. This normally happens when the port is wrong. Try entering another?"
            )
            user["port"] = get_val("port")
            fix_schema(user)
            return test_redshift(user)
        elif "password authentication failed" in e.args[0]:
            print("The credentials didn't work. Try others?")
            user["redshift_username"] = get_val("redshift_username")
            user["redshift_password"] = get_val("redshift_password")
            fix_schema(user)
            return test_redshift(user)
        else:
            raise BaseException

    cursor = conn.cursor()
    full_table_name = (
        f"{user['dbname']}.{user.get('default_schema', 'public')}.{table_name}"
    )
    try:
        cursor.execute(
            f"create table {full_table_name} (test_col varchar(10), test_col2 int)"
        )
    except psycopg2.errors.InvalidSchemaName:
        print("It looks like that schema doesn't exist. Want to specify another?")
        user["default_schema"] = get_val("default_schema")
        fix_schema(user)
        return test_redshift(user)
    except psycopg2.errors.InsufficientPrivilege:
        print(
            "It looks like you don't have permissions to create tables in this schema. Try another?"
        )
        user["default_schema"] = get_val("default_schema")
        fix_schema(user)
        return test_redshift(user)

    cursor.execute(f"insert into {full_table_name} values ('hi', 2)")
    cursor.execute(f"drop table {full_table_name}")

    conn.close()
    print("\tRedshift permissions tested successfully")


def test_connections(user):
    test_redshift(user)
    test_s3(user)


def test_vals(user):
    do_tests = "y"  # yes_no("Do you want to verify these values are correct?")
    if do_tests == "n":
        return
    fix_schema(user)
    print("Testing connections now")
    test_connections(user)
    print("Connections tested successfully")


user = {
    "host": "cluster-bud7ih5pdkbw.czdokwtvhved.us-east-1.redshift.amazonaws.com",
    "port": 5439,
    "dbname": "test",
    "redshift_username": "test",
    "redshift_password": "Testtes1",
    "default_schema": "public",
    "bucket": "test-library-store-1urzygqmztkxc",
    "access_key": "AKIARTZ6RUF6RY7VLCO7",
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
