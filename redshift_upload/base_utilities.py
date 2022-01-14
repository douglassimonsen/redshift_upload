import inspect
import os
from pathlib import Path


class change_directory:
    """
    A class for changing the working directory using a "with" statement.
    It takes the directory to change to as an argument. If no directory is given,
    it takes the directory of the file from which this function was called.
    """

    def __init__(self, directory: str = None) -> None:
        self.old_dir = os.getcwd()
        if directory is None:
            self.new_dir = Path(inspect.getmodule(inspect.stack()[1][0]).__file__).parent  # type: ignore
        else:
            self.new_dir = directory

    def __enter__(self, *_) -> None:
        os.chdir(self.new_dir)

    def __exit__(self, *_) -> None:
        os.chdir(self.old_dir)
