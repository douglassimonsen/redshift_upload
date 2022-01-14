import pytest
import sys, pathlib  # noqa

sys.path.insert(0, str(pathlib.Path(__file__).parents[1]))
from redshift_upload import credential_store


def pytest_addoption(parser):
    parser.addoption(
        "--schema",
        action="store",
        default="public",
        help="schema to run the upload tests in",
    )


@pytest.fixture(autouse=True)
def set_store():
    credential_store.set_store("test-library")


@pytest.fixture()
def schema(request):
    return request.config.getoption("--schema")
