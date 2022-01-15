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
5. To remove the Redshift environment after testing, run `python ./gen_environment/main.py --destroy`

### Run tests
Note: Due to the relatively slow nature of these tests, it's suggested you install `pip install pytest-xdist` in order to run these tests in parallel.

1. To run tests, just run `pytest` or `pytest -n auto --dist loadfile` (2nd is only available if you have pytest-xdist installed). The `--dist loadfile` is important. The tests in each file all target the same table and you will experience failures when multiple tests manipulate the same table.
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

# Credential Store

One of the common issues when connecting to databases is handling credentials. Although we'd ideally always store secrets in [AWS KMS](https://aws.amazon.com/kms/), often what happens is that credentials end up hardcoded in programs. Not only is this insecure, but it makes rotating credentials a monumental task.

The credential store is a middle ground between these two. By utilizing the credential store, the credentials are stored in a single json within the library itself. The credentials are stored in plain text, but it's a single location that won't be included in any git repositories and should be in a part of the filesystem that people rarely visit, making it _kind of_ secure.

## Setting up a Credential Store
__Note 1__: In case you need to access multiple DBs, the store can handle multiple sets of credentials.
__Note 2__: The store does basic pattern matching to ensure the data you entered matches the format the library needs.
```python
from redshift_upload import credential_store

sample_creds = {
    "host": "cluster.redshift.amazonaws.com",
    "port": 5439,
    "dbname": "test",
    "default_schema": "public",
    "redshift_username": "user",
    "redshift_password": "pass",
    "bucket": "bucket-name",
    "access_key": "AAAAAAAAAA0000000000",
    "secret_key": "AAAAAAAAAAAbbbbbbbbb999999999999999999/=",
}
credential_store.credentials['<name1>'] = sample_creds
credential_store.credentials['<name2>'] = sample_creds
```

## Accessing Credentials
__Note__: When you enter your first set of credentials, the store designates them as the default credentials. This can 
```python
from redshift_upload import credential_store
creds = credential_store.credentials['<name1>']
creds = credential_store.credentials()  # returns the default credentials
```

## Using Store in Upload
```python
import redshift_upload
redshift_upload.upload(
    source='tests/full_tests/load_source.csv',
    table_name="test",
)  # runs as the default user

redshift_upload.upload(
    source='tests/full_tests/load_source.csv',
    table_name="test",
    aws_info="<user1>",
)  # runs as the specified user
```

## Updating Default Credentials
__Note__: If you try to set the default to a user that doesn't exist, the store will raise a `ValueError`
```python
from redshift_upload import credential_store
credential_store.credentials.default = '<name2>'
```

## Deleting Credentials
```python
from redshift_upload import credential_store
del credential_store.credentials['<name2>']
```

## Removing Credential Store
```python
from redshift_upload import credential_store
credential_store.credentials.clear()  # replaces the store with an empty store and saves it to store.json
credential_store.credentials.delete()  # deletes store.json. This would mainly be used when you have a temporary credential store. The tests for this library use this function for cleanup, but I can't imagine why this would be used by end users. 
```

## Changing Active Credential Store
__Note 1__: The default store is named store.json and is set by default
__Note 2__: If you don't end the store with '.json', it will be automatically added
```python
from redshift_upload import credential_store
store_1 = credential_store.set_store('test')
store_2 = credential_store.set_store('test.json')
assert store_1.file_path == store_2.file_path
```
## Required permissions for library
The AWS keys for this library should have at least the following permissions
```json
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Action": [
                "s3:PutObject",
                "s3:GetObject",
                "s3:ListBucket",
                "s3:DeleteObject"
            ],
            "Resource": [
                "arn:aws:s3:::<bucket-name>/*",
                "arn:aws:s3:::<bucket-name>"
            ],
            "Effect": "Allow",
            "Sid": "basicS3Access"
        }
    ]

```
The redshift users must have access to the following system tables:
1. SVV_TABLE_INFO
2. STV_LOCKS
# Contributing

## Setup environment
This project uses a [Black](https://black.readthedocs.io/en/stable/) pre-commit hook to ensure consistent styling. To enable this, run `pre-commit install --hook-type pre-commit --hook-type post-commit` in the directory. This project uses flake8 with the following command: `flake8 --ignore E501,E402,E401,W503`


## Deploying a new verion to pypi
1. Update the version in `redshift_upload/__init__.py`
2. Run `python push.py`
5. Check [actions](https://github.com/douglassimonsen/redshift_upload/actions) for deployment progress

## Warnings
1. For the case of `varchar`, this program converts all empty strings to null. This was noticed late in development and probably can't be fixed :(