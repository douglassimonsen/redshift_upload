import timeit
import sys
import pathlib
import functools
import multiprocessing
import pandas
import matplotlib.pyplot as plt
import seaborn as sns
sys.path.insert(0, str(pathlib.Path(__file__).parents[2]))
from redshift_upload import upload, base_utilities  # noqa
from redshift_upload.db_interfaces import redshift
import json  # noqa
with base_utilities.change_directory():
    with open("../aws_creds.json") as f:
        aws_creds = json.load(f)
POWERS_CHECKED = 4
conn = redshift.Interface("public", "perf_test", aws_creds).get_db_conn()


def library(data, table_name):
    upload(
        source=data,
        schema_name="public",
        table_name=table_name,
        upload_options={
            "skip_checks": True,
            'default_logging': False,
        },
        aws_info=aws_creds
    )


def insert(data, table_name):
    insert_query = f'''
    insert into public.{table_name} (a)
    values (%(a)s)
    '''
    cursor = conn.cursor()
    cursor.executemany(insert_query, data)
    conn.commit()


def setup():
    query = '''
    drop table if exists public.perf_test_{method}_{power};
    create table public.perf_test_{method}_{power} (a varchar(2000));
    truncate table public.perf_test_{method}_{power};
    '''
    cursor = conn.cursor()
    for i in range(POWERS_CHECKED):
        cursor.execute(query.format(method='library', power=str(i)))
        cursor.execute(query.format(method='insert', power=str(i)))
        conn.commit()


def teardown():
    query = 'drop table if exists public.perf_test_{method}_{power};'
    cursor = conn.cursor()
    for i in range(POWERS_CHECKED):
        cursor.execute(query.format(method='library', power=str(i)))
        cursor.execute(query.format(method='insert', power=str(i)))
        conn.commit()


def gen_plot(data):
    data.columns = ['Rows Loaded', 'Time (Seconds)', 'Method']
    sns.set_context("paper")
    sns.set_style("darkgrid")
    sns.lineplot(
        data=data,
        x='Rows Loaded',
        y='Time (Seconds)',
        hue='Method',
        marker='o'
    )
    plt.title("Performance Comparison")
    plt.yscale("log")
    fig = plt.gcf()
    fig.set_size_inches(7, 4)
    with base_utilities.change_directory():
        fig.savefig("../../documentation/comparison.png", dpi=100)


def test_method(method, row_count, version):
    data = [{'a': 'hi' * 10} for _ in range(row_count)]
    table_name = f'perf_test_{method.__name__}_{version}'
    return {
        'rows': row_count,
        'time': timeit.timeit(functools.partial(method, data, table_name), number=1),
        'method': method.__name__.capitalize()
    }


def main():
    results = []
    setup()
    row_counts = [2 ** (x * 2) for x in range(POWERS_CHECKED)]

    test_method(library, row_counts[0], 0)
    teardown()

    results = pandas.DataFrame(results)
    gen_plot(results)


if __name__ == '__main__':
    main()
    conn.close()