import timeit
import sys
import pathlib
import functools
import logging
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
query = '''
drop table if exists public.perf_test;
create table public.perf_test (a varchar(2000));
truncate table public.perf_test;
'''
conn = redshift.Interface("public", "perf_test", aws_creds).get_db_conn()
cursor = conn.cursor()
cursor.execute(query)
conn.commit()


def library(data):
    upload(
        source=data,
        schema_name="public",
        table_name="perf_test",
        upload_options={
            "skip_checks": True,
            'default_logging': False,
        },
        aws_info=aws_creds
    )


def insert(data):
    insert_query = '''
    insert into public.perf_test (a)
    values (%(a)s)
    '''
    cursor = conn.cursor()
    cursor.executemany(insert_query, data)
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


def main():
    log = logging.getLogger("redshift_utilities")
    tries = 1
    results = []
    for power_of_two in range(4):
        rows = 2 ** (power_of_two * 2)
        print(f"Testing against {rows} rows")
        data = [{'a': 'hi' * 10} for _ in range(rows)]
        results.append({
            'rows': rows,
            'time': timeit.timeit(functools.partial(library, data), number=tries),
            'method': 'Library',
        })
        results.append({
            'rows': rows,
            'time': timeit.timeit(functools.partial(insert, data), number=tries),
            'method': 'Insert',
        })
    results = pandas.DataFrame(results)
    gen_plot(results)


if __name__ == '__main__':
    main()
    conn.close()