import pytest


def pytest_addoption(parser):
    parser.addoption(
        "--schema",
        action="store",
        default="public",
        help="schema to run the upload tests in",
    )


@pytest.fixture()
def schema(request):
    return request.config.getoption("--schema")
