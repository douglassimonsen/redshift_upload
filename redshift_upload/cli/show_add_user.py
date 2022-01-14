try:
    from ..credential_store import credential_store
except ImportError:
    import sys
    from pathlib import Path

    sys.path.insert(0, Path(__file__).parents[1])
    from credential_store import credential_store
import jsonschema
import psycopg2
import boto3
import random
import botocore.exceptions
import botocore.errorfactory
import colorama
from typing import Callable, Dict, List

colorama.init()
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
        "logging_endpoint",
        "logging_endpoint_type",
        "get_table_lock",
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
        "constants": {
            "default_schema": "public",
            "logging_endpoint_type": "db",
            "get_table_lock": True,
        },
        "s3": {},
    }
s3_name = "library_test/" + "".join(
    random.choices([chr(65 + i) for i in range(26)], k=20)
)  # needs to be out here so repeated s3 checks don't create orphan objects
table_name = "library_test_" + "".join(
    random.choices([chr(65 + i) for i in range(26)], k=20)
)  # needs to be out here so repeated redshift checks don't create orphan tables


def colorize(text: str, level: str = "INFO") -> str:
    level_format = {
        "INFO": colorama.Style.BRIGHT + colorama.Fore.CYAN,
        "SUCCESS": colorama.Fore.GREEN,
        "WARNING": colorama.Fore.YELLOW,
        "ERROR": colorama.Fore.RED,
    }
    return level_format[level] + text + colorama.Style.RESET_ALL


def get_val(section: str, param: str) -> str | int | bool | None:
    question = f"What is the value for {param}"
    default_val = None

    if param in default_params[section]:
        default_val = default_params[section][param]
        question += f" (default: {default_val})"

    question += ": "
    ret = input(colorize(question))
    if len(ret) == 0 and default_val is not None:
        return default_val
    formatting_options = {
        "port": lambda x: int(x),
        "logging_endpoint": lambda x: x or None,
        "logging_endpoint_type": lambda x: x or None,
        "get_table_lock": lambda x: (x.lower()[0] == "t"),
    }
    return formatting_options.get(param, lambda x: x)(ret)


def yes_no(question: str) -> str:
    raw = input(colorize(f"{question} (y/n): ")).lower()
    if raw not in ("y", "n"):
        return yes_no(question)
    return raw


def fix_schema(user: Dict) -> None:
    print("Testing it matches the credential JSONSchema...")
    try:
        jsonschema.validate(user, credential_store.SCHEMA)
        print(colorize("Schema successfully validated!", "SUCCESS"))
        return
    except jsonschema.exceptions.ValidationError as e:
        print(colorize(f"{e.path[-1]}: {e.message}", "WARNING"))
        user[e.path[0]][e.path[1]] = get_val(e.path[0], e.path[1])
        return fix_schema(user)


def unhandled_aws_error(error: BaseException) -> None:
    print(colorize("Unhandled error :(", "ERROR"))
    print(error)
    print(error.response)
    raise ValueError


def test_failed(
    error_msg: str, bad_params: List[List[str]], user: Dict, test_func: Callable
) -> None:
    print(colorize(error_msg, "WARNING"))
    for param in bad_params:
        user[param[0]][param[1]] = get_val(param[0], param[1])
        fix_schema(user)
        return test_func(user)


def test_s3(user: Dict) -> None:
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
            return test_failed(
                "It looks like the access key doesn't exist. Try another?",
                [["s3", "access_key"], ["s3", "secret_key"]],
                user,
                test_s3,
            )
        elif (
            e.response["Error"]["Code"] == "AccessDenied"
            and e.operation_name == "PutObject"
        ):
            return test_failed(
                "It looks like the access key doesn't have permission to write to the specified bucket. Try new access keys or bucket",
                [["s3", "access_key"], ["s3", "secret_key"], ["constants", "bucket"]],
                user,
                test_s3,
            )
        elif e.response["Error"]["Code"] == "NoSuchBucket":
            return test_failed(
                "It looks like that bucket doesn't exist. Try another?",
                [["constants", "bucket"]],
                user,
                test_s3,
            )
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


def test_redshift(user: Dict) -> None:
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
            user["db"]["dbname"] = get_val("db", "dbname")
            fix_schema(user)
            return test_redshift(user)
        elif "Unknown host" in e.args[0]:
            print(
                colorize(
                    "It looks like that host doesn't exist. Try entering another?",
                    "WARNING",
                )
            )
            user["db"]["host"] = get_val("db", "host")
            fix_schema(user)
            return test_redshift(user)
        elif "timeout expired" in e.args[0]:
            return test_failed(
                "The connection timed out. This normally happens when the port is wrong. Try entering another?",
                [["db", "port"]],
                user,
                test_redshift,
            )
        elif "password authentication failed" in e.args[0]:
            return test_failed(
                "The credentials failed authentication. Try others?",
                [["db", "user"], ["db", "password"]],
                user,
                test_redshift,
            )
        else:
            raise BaseException

    cursor = conn.cursor()
    full_table_name = f"{user['db']['dbname']}.{user['constants'].get('default_schema', 'public')}.{table_name}"
    try:
        cursor.execute(
            f"create table {full_table_name} (test_col varchar(10), test_col2 int)"
        )
    except psycopg2.errors.InvalidSchemaName:
        return test_failed(
            "It looks like that schema doesn't exist. Want to specify another?",
            [["constants", "default_schema"]],
            user,
            test_redshift,
        )
    except psycopg2.errors.InsufficientPrivilege:
        return test_failed(
            "It looks like you don't have permissions to create tables in this schema. Try another?",
            [["constants", "default_schema"]],
            user,
            test_redshift,
        )

    cursor.execute(f"insert into {full_table_name} values ('hi', 2)")
    cursor.execute(f"drop table {full_table_name}")

    conn.close()
    print(colorize("Redshift permissions tested successfully", "SUCCESS"))


def test_connections(user) -> None:
    test_redshift(user)
    test_s3(user)


def test_vals(user) -> None:
    do_tests = yes_no("Do you want to verify these values are correct?")
    if do_tests == "n":
        return
    fix_schema(user)
    print(colorize("Testing connections now"))
    test_connections(user)
    print(colorize("Connections tested successfully", "SUCCESS"))


def main() -> None:
    print(intro)
    user = {"s3": {}, "db": {}, "constants": {}}
    for section, params in param_sections.items():
        for param in params:
            user[section][param] = get_val(section, param)
    print(colorize("This is the data you've entered:"))
    print("\n" + json.dumps(user, indent=4) + "\n\n")
    test_vals(user)


if __name__ == "__main__":
    main()
