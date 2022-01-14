import timeit
import sys
import pathlib
import functools
import multiprocessing
import pandas
import matplotlib.pyplot as plt
import seaborn as sns
import psycopg2.extras
import psycopg2

sys.path.insert(0, str(pathlib.Path(__file__).parents[2]))
from redshift_upload import upload, base_utilities  # noqa
import json  # noqa

with base_utilities.change_directory():
    with open("../aws_creds.json") as f:
        aws_creds = json.load(f)
POWERS_CHECKED = 8


def get_conn():
    return psycopg2.connect(**aws_creds["db"])


def library(data, table_name):
    upload(
        source=data,
        schema_name="public",
        table_name=table_name,
        upload_options={
            "skip_checks": True,
            "default_logging": False,
        },
        aws_info=aws_creds,
    )


def naive_insert(data, table_name):
    insert_query = f"""
    insert into public.{table_name} (a)
    values (%(a)s)
    """
    with get_conn() as conn:
        cursor = conn.cursor()
        cursor.executemany(insert_query, data)
        conn.commit()


def batch_insert(data, table_name):
    insert_query = f"""
    insert into public.{table_name} (a)
    values (%(a)s)
    """
    with get_conn() as conn:
        cursor = conn.cursor()
        psycopg2.extras.execute_batch(cursor, insert_query, data)


def setup():
    query = """
    drop table if exists public.perf_test_{method}_{power};
    create table public.perf_test_{method}_{power} (a varchar(2000));
    truncate table public.perf_test_{method}_{power};
    """
    with get_conn() as conn:
        cursor = conn.cursor()
        for i in range(POWERS_CHECKED):
            for method in ["library", "naive_insert", "batch_insert"]:
                cursor.execute(query.format(method=method, power=str(i)))
        conn.commit()


def teardown():
    query = "drop table if exists public.perf_test_{method}_{power};"
    with get_conn() as conn:
        cursor = conn.cursor()
        for i in range(POWERS_CHECKED):
            for method in ["library", "naive_insert", "batch_insert"]:
                cursor.execute(query.format(method=method, power=str(i)))
        conn.commit()


def gen_plot(data):
    data.columns = ["Rows Loaded", "Time (Seconds)", "Method"]
    sns.set_context("paper")
    sns.set_style("darkgrid")
    sns.lineplot(
        data=data, x="Rows Loaded", y="Time (Seconds)", hue="Method", marker="o"
    )
    plt.title("Performance Comparison")
    plt.yscale("log")
    fig = plt.gcf()
    fig.set_size_inches(7, 4)
    with base_utilities.change_directory():
        fig.savefig("../../documentation/comparison.png", dpi=100)


def test_method(args):
    def pretty(name):
        name = method.__name__.replace("_", " ").split()
        name = [x.capitalize() for x in name]
        return " ".join(name)

    method, row_count, version = args
    print(row_count, method.__name__, "start")
    data = [{"a": "hi" * 10} for _ in range(row_count)]
    table_name = f"perf_test_{method.__name__}_{version}"
    if row_count < 10_000:
        number = 1
    elif row_count < 100_000:
        number = 1
    else:
        number = 1
    ret = {
        "rows": row_count,
        "time": timeit.timeit(
            functools.partial(method, data, table_name), number=number
        ),
        "method": pretty(method),
    }
    print(row_count, method.__name__, "finished")
    return ret


def main():
    setup()
    row_counts = [10 ** x for x in range(POWERS_CHECKED)]
    library_data = [(library, rows, i) for i, rows in enumerate(row_counts)]
    naive_insert_data = [
        (naive_insert, rows, i) for i, rows in enumerate(row_counts[:4])
    ]
    batch_insert_data = [
        (batch_insert, rows, i) for i, rows in enumerate(row_counts[:4])
    ]
    data = naive_insert_data + batch_insert_data + library_data
    results = []
    with multiprocessing.Pool(processes=8) as pool:
        results.extend(pool.map(test_method, data))
    teardown()

    results = pandas.DataFrame(results).sort_values("rows")
    results["rows"] = results["rows"].apply(lambda x: f"{x:,}")
    gen_plot(results)


if __name__ == "__main__":
    main()
