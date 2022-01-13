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
import colorama
import json


param_sections = {
    "s3": [
        "access_key",
        "secret_key",
    ],
    "db": [
        "host",
        "port",
        "dbname",
        "user",
        "password",
    ],
    "constants": [
        "default_schema",
        "bucket",
    ],
}


intro = """
Let's get you uploading! The library needs these credentials to function.
If you already have a default account set up in store.json, you can press
enter to accept the default for that param.
"""
default_user = credential_store.credentials.default
if default_user is not None:
    default_params = credential_store.credentials[default_user]
else:
    default_params = {
        "db": {"port": 5439},
        "constants": {"default_schema": "public"},
        "s3": {},
    }
s3_name = "library_test/" + "".join(
    random.choices([chr(65 + i) for i in range(26)], k=20)
)  # needs to be out here so repeated s3 checks don't create orphan objects
table_name = "library_test_" + "".join(
    random.choices([chr(65 + i) for i in range(26)], k=20)
)  # needs to be out here so repeated redshift checks don't create orphan objects


def colorize(text, level="INFO"):
    level_format = {
        "INFO": colorama.Style.BRIGHT + colorama.Fore.CYAN,
        "SUCCESS": colorama.Fore.GREEN,
        "WARNING": colorama.Fore.YELLOW,
        "ERROR": colorama.Fore.RED,
    }
    return level_format[level] + text + colorama.Style.RESET_ALL


def get_val(param, section):
    question = f"What is the value for {param}"
    default_val = None

    if param in default_params[section]:
        default_val = default_params[section][param]
        question += f" (default: {default_val})"

    question += ": "
    ret = input(colorize(question))
    if len(ret) == 0 and default_val is not None:
        return default_val
    if param == "port":
        ret = int(ret)
    return ret


def yes_no(question):
    raw = input(colorize(f"{question} (y/n): ")).lower()
    if raw not in ("y", "n"):
        return yes_no(question)
    return raw


def fix_schema(user):
    print("Testing it matches the credential JSONSchema...")
    try:
        jsonschema.validate(user, credential_store.SCHEMA)
        print(colorize("Schema successfully validated!", "SUCCESS"))
        return
    except jsonschema.exceptions.ValidationError as e:
        print(colorize(f"{e.path[-1]}: {e.message}", "WARNING"))
        user[e.path[0]][e.path[1]] = get_val(e.path[1], e.path[0])
        return fix_schema(user)


def unhandled_aws_error(error):
    print(colorize("Unhandled error :(", "ERROR"))
    print(error)
    print(error.response)
    raise ValueError


def test_s3(user):
    # test accesss/secret key
    # test bucket can be written to
    # test bucket can be deleted from
    print(colorize("Testing S3 permissions"))
    s3 = boto3.resource(
        "s3",
        aws_access_key_id=user["s3"]["access_key"],
        aws_secret_access_key=user["s3"]["secret_key"],
    )
    obj = s3.Object(user["constants"]["bucket"], s3_name)
    try:
        obj.put(Body=b"test")
    except botocore.exceptions.ClientError as e:
        if e.response["Error"]["Code"] == "InvalidAccessKeyId":
            print(
                colorize(
                    "It looks like the access key doesn't exist. Try another?",
                    "WARNING",
                )
            )
            user["s3"]["access_key"] = get_val("access_key", "s3")
            user["s3"]["secret_key"] = get_val("secret_key", "s3")
            fix_schema(user)
            return test_s3(user)
        elif (
            e.response["Error"]["Code"] == "AccessDenied"
            and e.operation_name == "PutObject"
        ):
            print(
                colorize(
                    "It looks like the access key doesn't have permission to write to the specified bucket. Try new access keys or bucket",
                    "WARNING",
                )
            )
            user["s3"]["access_key"] = get_val("access_key", "s3")
            user["s3"]["secret_key"] = get_val("secret_key", "s3")
            fix_schema(user)
            return test_s3(user)
        elif e.response["Error"]["Code"] == "NoSuchBucket":
            print(
                colorize(
                    "It looks like that bucket doesn't exist. Try another?", "WARNING"
                )
            )
            user["constants"]["bucket"] = get_val("bucket", "constants")
            fix_schema(user)
            return test_s3(user)
        else:
            unhandled_aws_error(e)

    try:
        obj.delete()
    except botocore.exceptions.ClientError as e:
        if e.response["Error"]["Code"] == "AccessDenied":
            print(
                colorize(
                    "It looks like the access key can't delete things. That's fine, the library will overwrite the blob with a blank body so it doesn't bloat costs",
                    "WARNING",
                )
            )
        else:
            unhandled_aws_error(e)

    print(colorize("S3 permissions tested successfully", "SUCCESS"))


def test_redshift(user):
    # test create/delete table
    print(colorize("Testing Redshift Permissions"))
    try:
        conn = psycopg2.connect(
            **user["db"],
            connect_timeout=5,
        )
    except psycopg2.OperationalError as e:
        if "FATAL:  database" in e.args[0]:
            print(
                colorize(
                    "It looks like that database doesn't exist. Try entering another?",
                    "WARNING",
                )
            )
            user["db"]["dbname"] = get_val("dbname", "db")
            fix_schema(user)
            return test_redshift(user)
        elif "Unknown host" in e.args[0]:
            print(
                colorize(
                    "It looks like that host doesn't exist. Try entering another?",
                    "WARNING",
                )
            )
            user["db"]["host"] = get_val("host", "db")
            fix_schema(user)
            return test_redshift(user)
        elif "timeout expired" in e.args[0]:
            print(
                colorize(
                    "The connection timed out. This normally happens when the port is wrong. Try entering another?",
                    "WARNING",
                )
            )
            user["db"]["port"] = get_val("port", "db")
            fix_schema(user)
            return test_redshift(user)
        elif "password authentication failed" in e.args[0]:
            print(colorize("The credentials didn't work. Try others?", "WARNING"))
            user["db"]["user"] = get_val("user", "db")
            user["db"]["password"] = get_val("password", "db")
            fix_schema(user)
            return test_redshift(user)
        else:
            raise BaseException

    cursor = conn.cursor()
    full_table_name = f"{user['db']['dbname']}.{user['constants'].get('default_schema', 'public')}.{table_name}"
    try:
        cursor.execute(
            f"create table {full_table_name} (test_col varchar(10), test_col2 int)"
        )
    except psycopg2.errors.InvalidSchemaName:
        print(
            colorize(
                "It looks like that schema doesn't exist. Want to specify another?",
                "WARNING",
            )
        )
        user["constants"]["default_schema"] = get_val("default_schema", "constants")
        fix_schema(user)
        return test_redshift(user)
    except psycopg2.errors.InsufficientPrivilege:
        print(
            colorize(
                "It looks like you don't have permissions to create tables in this schema. Try another?",
                "WARNING",
            )
        )
        user["constants"]["default_schema"] = get_val("default_schema", "constants")
        fix_schema(user)
        return test_redshift(user)

    cursor.execute(f"insert into {full_table_name} values ('hi', 2)")
    cursor.execute(f"drop table {full_table_name}")

    conn.close()
    print(colorize("Redshift permissions tested successfully", "SUCCESS"))


def test_connections(user):
    test_redshift(user)
    test_s3(user)


def test_vals(user):
    do_tests = yes_no("Do you want to verify these values are correct?")
    if do_tests == "n":
        return
    fix_schema(user)
    print(colorize("Testing connections now"))
    test_connections(user)
    print(colorize("Connections tested successfully", "SUCCESS"))


def main():
    print(intro)
    user = {"s3": {}, "db": {}, "constants": {}}
    for section, params in param_sections.items():
        for param in params:
            user[section][param] = get_val(param, section)
    print(colorize("This is the data you've entered:"))
    print("\n" + json.dumps(user, indent=4) + "\n\n")
    test_vals(user)


if __name__ == "__main__":
    main()
