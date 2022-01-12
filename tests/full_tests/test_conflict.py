try:
    from redshift_upload import upload, credential_store, testing_utilities  # noqa
except ModuleNotFoundError:
    import sys, os

    sys.path.insert(
        0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    )
    from redshift_upload import upload, credential_store, testing_utilities  # noqa
import pytest
import psycopg2

table_name = (
    "unit_" + __file__.replace("\\", "/").split("/")[-1].split(".")[0]
)  # we would just use __name__, but we don't want to run into __main__ if called directly
# converts test_conflict.py -> unit_test_confict
df = [{"a": 1}, {"a": 2}, {"a": 3}]


@pytest.fixture(autouse=True)
def setup_and_teardown():
    testing_utilities.drop_tables(table_name)
    yield  # this pauses the function for the tests to runa
    testing_utilities.drop_tables(table_name)


def test_conflict():
    upload(
        source=df,
        table_name=table_name,
        aws_info="analyst1",
    )
    user2 = credential_store.credentials.profiles["analyst2"]
    with psycopg2.connect(
        host=user2["host"],
        dbname=user2["dbname"],
        port=user2["port"],
        user=user2["redshift_username"],
        password=user2["redshift_password"],
        connect_timeout=5,
    ) as conn:
        cursor = conn.cursor()
        cursor.execute(
            f"create view public.unit_test_conflict_view as (select * from public.{table_name})"
        )
    upload(
        source=df,
        table_name=table_name,
        aws_info="analyst1",
    )


if __name__ == "__main__":
    test_conflict()
