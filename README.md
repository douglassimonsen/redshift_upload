Install this package with `pip install simple_redshift_upload`

## Test
1. Clone this repository
2. Using the file `aws_account_creds_template.json`, fill in the data and rename the file `aws_account_creds.json`
3. Run the file `gen_redshift_environment.py --start`
4. Run the file `tests\run.py`
5. To remove the Redshift environment after testing, run `gen_redshift_environment.py --end`


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
