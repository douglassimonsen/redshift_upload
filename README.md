Install this package with `pip install simple_redshift_upload`

## Testing
### Set up environment
Way 1 (Assumes you have a set up environment)
1. Clone this repository
2. `cd` into the directory
3. Using the file `aws_account_creds_template.json`, fill in the data and rename the file `aws_account_creds.json`
4. Run the file `gen_redshift_environment.py --start`
5. Run the tests
6. To remove the Redshift environment after testing, run `gen_redshift_environment.py --end`

Way 2 (Blank Slate test environment)
1. Clone this repository
2. `cd` into the directory
3. Run the command `python ./gen_environment/main.py`. This script does the following:
    1. Runs `aws cloudformation deploy --template-file ./gen_environment/template.yaml --stack-name test`
    2. Generates access key pairs with access to the S3 bucket
    3. Creates temporary accounts in Redshift
    4. Creates a creds.json with the associated credentials.
4. Run the tests
5. To remove the Redshift environment after testing, run `aws cloudformation delete-stack --stack-name test

### Run tests
Note: Due to the relatively slow nature of these tests, it's suggested you install `pip install pytest-xdist` in order to run these tests in parallel.

1. To run tests, just run `pytest` or `pytest -n --dist loadfile` (2nd is only available if you have pytest-xdist installed)
2. To test mypy, run the command `mypy -p redshift_upload`
    1. There should be 10 errors about Optional Dictionaries not being indexable in upload.py. Those are ignorable.
3. To run the performance test, just run `python ./tests/performance/base.py`

## High Level Process
This package follows the following steps to upload your data to Redshift.

1. Gets the data and makes it into a pandas.DataFrame
2. Using locally defined columns, remote columns (if the table exists and isn't going to be dropped) and type checking, serializes the columns.
3. Checks the remote to add any columns that are in the remote, but not local. If there are varchar columns that are too small to fit the new data, the program attempts to expand the varchar column
4. If the table is going to be dropped, looks for dependent views. It saves the dependent views locally and metadata like the view's dependencies
5. Loads the data to s3. If load_in_parallel > 1, it splits it into groups to speed up upload.
6. Deletes/Truncates the table if specified .
7. Copies the data from s3 to Redshift
8. Grants access to the specified individuals/groups
9. If necessary, re-instantiates the dependent views, using toposort to generate the topological ordering of the dependencies
10. If a records table has been specified, records basic information about the upload
11. Cleans up the S3 files, if specified
12. Returns the interface object, in case you want to see more data or use the connection to the db to continue querying
![Library Workflow](https://github.com/douglassimonsen/redshift_upload/blob/main/documentation/process_flow.png)
## Example
```python3
df = pandas.DataFrame([{"a": "hi"}, {"a": "hi"}])
aws_creds = {
    "redshift_username": "",
    "redshift_password": "",
    "access_key": "",
    "secret_key": "",
    "bucket": "",
    "host": "",
    "dbname": "",
    "port": ""
}


upload.upload(
    source=df,
    schema_name="public",
    table_name="unit_test_column_expansion",
    upload_options={"drop_table": True},
    aws_info=aws_creds,
)
```

# Performance Comparison
Given that there are other, simpler ways to upload data to Redshift, we should compare the various methods. Using a simple table with a single varchar column, we upload using the following methods:

__Naive Insert__ 
```python
def naive_insert(data, table_name):
    insert_query = f'''
    insert into public.{table_name} (a)
    values (%(a)s)
    '''
    with get_conn() as conn:
        cursor = conn.cursor()
        cursor.executemany(insert_query, data)
        conn.commit()
```

__Batch Insert__
```python
def batch_insert(data, table_name):
    insert_query = f'''
    insert into public.{table_name} (a)
    values (%(a)s)
    '''
    with get_conn() as conn:
        cursor = conn.cursor()
        psycopg2.extras.execute_batch(cursor, insert_query, data)
```

__Library__
```python
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
```

![Performance Comparison](https://github.com/douglassimonsen/redshift_upload/blob/main/documentation/comparison.png)