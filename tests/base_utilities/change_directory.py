import sys
import pathlib
import os
sys.path.insert(0, str(pathlib.Path(__file__).parents[2]))
from redshift_upload import base_utilities  # noqa


def test_change_directory():
    with base_utilities.change_directory():
        assert os.getcwd().replace('\\', '/').endswith('redshift_upload/tests/base_utilities')


if __name__ == '__main__':
    test_change_directory()
