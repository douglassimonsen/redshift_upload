from statistics import mode


try:
    from .upload import upload
    from . import base_utilities
    from .credential_store import credential_store
except ModuleNotFoundError:  # needed for when setup.py imports the __version__
    pass
__version__ = "1.1.9"
