import sys
import pathlib
import os
sys.path.insert(0, str(pathlib.Path(__file__).parents[2]))
import base_utilities


def test_change_directory():
    with base_utilities.change_directory():
        assert os.getcwd().replace('\\', '/').endswith('redshift_upload/tests/base_utilities')
