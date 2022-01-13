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


def get_conn(user):
    return psycopg2.connect(
        **user["db"],
        connect_timeout=5,
    )


def test_conflict():
    upload(
        source=df,
        table_name=table_name,
        aws_info="analyst1",
    )
    user2 = credential_store.credentials.profiles["analyst2"]
    with get_conn(user2) as conn:
        cursor = conn.cursor()
        cursor.execute(
            f"create view public.unit_test_conflict_view as (select * from public.{table_name})"
        )
    upload(
        source=df,
        table_name=table_name,
        aws_info="analyst1",
        upload_options={"close_on_end": False, "drop_table": True},
    )
    with get_conn(user2) as conn:
        cursor = conn.cursor()
        cursor.execute(
            """
            select pg_views.viewowner
            from pg_class

            left join pg_namespace 
            on pg_namespace.oid = pg_class.relnamespace

            left JOIN pg_views 
            on pg_views.schemaname = pg_namespace.nspname 
            and pg_views.viewname = pg_class.relname
            where relname = 'unit_test_conflict_view'
            """
        )
        owner = cursor.fetchone()
        if owner is None:
            raise ValueError("The view has disappeared")
        elif owner[0] != "analyst2":
            raise ValueError("The view isn't re-assigned to the correct user")


if __name__ == "__main__":
    credential_store.set_store("test-library")
    testing_utilities.drop_tables(table_name)
    test_conflict()
    # testing_utilities.drop_tables(table_name)
