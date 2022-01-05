import json
import os
try:
    from .. import base_utilities
except:
    import sys; sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    import base_utilities


def get_serialized_store(file_patb):
    with base_utilities.change_directory():
        if not os.path.exists(file_patb):
            return {
                'default': None,
                'profiles': {},
            }
        with open(file_patb, 'r') as f:
            return json.load(f)

class Store:
    def __init__(self, file_path='store.json') -> None:
        """
        file_path: string
        The filepath to load and save the store. Can be relative to credential_store.py file directory
        """
        if not file_path.endswith(".json"):
            file_path += '.json'
        self.file_path = file_path
        store = get_serialized_store(file_path)
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

    def __bool__(self):
        return bool(self.profiles)

    def __str__(self):
        return f'''
        default: {self.default}
        profiles: {list(self.profiles.keys())}
        '''

    def _save(self):
        with base_utilities.change_directory():
            with open(self.file_path, "w") as f:
                json.dump({
                    'profiles': self.profiles,
                    'default': self.default
                }, f, indent=4)

    def delete(self):
        with base_utilities.change_directory():
            if os.path.exists(self.file_path):  # I know this is *technically* a race condition, but I don't like try/except
                os.remove(self.file_path)

    def clear(self):
        self.default = None
        self.profiles = {}
        self._save()


def set_store(store):
    global credentials
    credentials = Store(store)


credentials = Store()
