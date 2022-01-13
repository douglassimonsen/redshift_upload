import json
import os
import jsonschema

try:
    from .. import base_utilities
except:
    import sys

    sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    import base_utilities


SCHEMA = {
    "type": "object",
    "properties": {
        "constants": {
            "type": "object",
            "properties": {
                "bucket": {
                    "type": "string",
                    "pattern": "(?=^.{3,63}$)(?!xn--)([a-z0-9](?:[a-z0-9-]*)[a-z0-9])$",
                },  # https://stackoverflow.com/a/62673054/6465644
                "default_schema": {"type": ["string", "null"]},
                "logging_endpoint": {"type": ["string", "null"]},
            },
            "additionalProperties": False,
            "required": ["bucket", "default_schema", "logging_endpoint"],
        },
        "s3": {
            "type": "object",
            "properties": {
                "access_key": {
                    "type": "string",
                    "pattern": "[A-Z0-9]{20}",
                },  # it's just all CAPS and numbers
                "secret_key": {
                    "type": "string",
                    "pattern": "[A-Za-z0-9/+=]{40}",
                },  # it's a base-64 string
            },
            "additionalProperties": False,
            "required": ["access_key", "secret_key"],
        },
        "db": {
            "properties": {
                "host": {"type": "string"},
                "port": {
                    "type": "integer",
                    "minimum": 1150,
                    "maximum": 65535,
                },  # port range found in the web creation wizard page
                "dbname": {"type": "string"},
                "user": {"type": "string"},
                "password": {"type": "string"},
            },
            "additionalProperties": False,
            "required": ["host", "port", "dbname", "user", "password"],
        },
    },
    "required": [
        "constants",
        "s3",
        "db",
    ],
    "additionalProperties": False,
}

assert set(SCHEMA["properties"].keys()) == set(SCHEMA["required"])
for property in SCHEMA["required"]:
    base = SCHEMA["properties"][property]
    assert set(base["properties"].keys()) == set(base["required"])


def _get_serialized_store(file_path):
    with base_utilities.change_directory():
        if not os.path.exists(file_path):
            return {
                "default": None,
                "profiles": {},
            }
        with open(file_path, "r") as f:
            return json.load(f)


class Store:
    def __init__(self, file_path="store.json") -> None:
        """
        file_path: string
        The filepath to load and save the store. Can be relative to credential_store.py file directory
        """
        if not file_path.endswith(".json"):
            file_path += ".json"
        self.file_path = file_path
        store = _get_serialized_store(file_path)
        self.profiles = store["profiles"]
        self.default = store["default"]

    def __getitem__(self, key):
        if not isinstance(key, str):
            raise KeyError(
                f"The profile name must be a string. You passed the value: {key}"
            )
        try:
            return self.profiles[key]
        except KeyError:
            raise KeyError("That profile does not exist in the store")

    def __setitem__(self, key, profile):
        raise KeyError("You're not allowed to directly set to the credential store")

    def add(self, profile):
        jsonschema.validate(instance=profile, schema=SCHEMA)
        self.profiles[profile["db"]["user"]] = profile
        if self.default is None:
            self.default = profile["db"]["user"]
        self._save()

    def __delitem__(self, key):
        del self.profiles[key]
        if not self.profiles:
            self.default = None
        elif key == self.default:
            self.default = next(
                self.profiles.__iter__()
            )  # way overcomplicated, but I wanted to show I knew how to do it efficiently. This just gets the next key in the dict
        self._save()

    def __setattr__(self, name: str, value: any) -> None:
        if name == "default":
            if value is not None and value not in self.profiles:
                raise ValueError(
                    f"The user '{value}' does not have a profile in this store."
                )

        super(Store, self).__setattr__(name, value)

    def __call__(self):
        return self.__getitem__(self.default)

    def __bool__(self):
        return bool(self.profiles)

    def __str__(self):
        return f"""
        default: {self.default}
        profiles: {list(self.profiles.keys())}
        """

    def _save(self):
        with base_utilities.change_directory():
            with open(self.file_path, "w") as f:
                json.dump(
                    {"profiles": self.profiles, "default": self.default}, f, indent=4
                )

    def delete(self):
        with base_utilities.change_directory():
            if os.path.exists(
                self.file_path
            ):  # I know this is *technically* a race condition, but I don't like try/except
                os.remove(self.file_path)

    def clear(self):
        self.default = None
        self.profiles = {}
        self._save()


def set_store(store):
    global credentials
    credentials = Store(store)


credentials = Store()
