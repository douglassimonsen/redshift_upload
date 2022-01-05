import json
try:
    from . import base_utilities
except:
    import sys, os; sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    import base_utilities


def get_serialized_store():
    with base_utilities.change_directory():
        if not os.path.exists('store.json'):
            return {
                'default': None,
                'profiles': {},
            }
        with open('store.json', 'r') as f:
            return json.load(f)

class Store:
    def __init__(self) -> None:
        store = get_serialized_store()
        self.profiles = store['profiles']
        self.default = store['default']

    def __getitem__(self, key):
        if not isinstance(key, str):
            raise KeyError(f"The profile name must be a string. You passed the value: {key}")
        try:
            return self.profiles[key]
        except KeyError:
            raise KeyError("That profile does not exist in the store")

    def __setitem__(self, key, profile):
        self.profiles[key] = profile
        if self.default is None:
            self.default = key
        self._save()

    def __delitem__(self, key):
        del self.profiles[key]
        if not self.profiles:
            self.default = None
        elif key == self.default:
            self.default = next(self.profiles.__iter__())  # way overcomplicated, but I wanted to show I knew how to do it efficiently. This just gets the next key in the dict
        self._save()

    def __call__(self):
        return self.__getitem__(self.default)

    def _save(self):
        with base_utilities.change_directory():
            with open("store.json", "w") as f:
                json.dump({
                    'profiles': self.profiles,
                    'default': self.default
                }, f, indent=4)


credentials = Store()
