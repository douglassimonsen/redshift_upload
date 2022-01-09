from ..credential_store import credential_store
import jsonschema

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
    try:
        jsonschema.validate(user, credential_store.SCHEMA)
        return
    except jsonschema.exceptions.ValidationError as e:
        print(f"{e.path[0]}: {e.message}")
        user[e.path[0]] = get_val(e.path[0])
        fix_schema(user)


def test_s3():
    # test accesss/secret key
    # test bucket can be written to
    # test bucket can be deleted from
    pass


def test_redshift():
    # test create/delete table
    pass


def test_connections(user):
    test_s3()
    test_redshift()


def test_vals(user):
    do_tests = "y"  # yes_no("Do you want to verify these values are correct?")
    if do_tests == "n":
        return
    print("Testing it matches the credential JSONSchema...")
    fix_schema(user)
    print("Schema successfully validated!")
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
