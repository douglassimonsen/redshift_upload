import sys
if '--help' in sys.argv:
    print("""
The upload_options dictionary contains the following possible keys:

truncate_table:
Default: False
Tells the program to run "truncate <table>" before copying the data

drop_table:
Default: False
Tells the program to run "drop table <table>; create table <table>" before copying data

cleanup_s3:
Default: True
Tells the program to try to delete the file in S3 after copying to Redshift

grant_access:
Default: []
A list of individuals/groups to grant select access to table

diststyle:
Default: "even"
The diststyle for a table. See https://docs.aws.amazon.com/redshift/latest/dg/c_choosing_dist_sort.html for more details on options

distkey:
Default: None
The column to distribute the table based on. Only allowed when diststyle = "key"

sortkey:
Default: None
The column to sort the table on

load_in_parallel:
Default: 1
The number of s3 files to seperate the file into. See more for why we do this here: https://docs.aws.amazon.com/redshift/latest/dg/t_splitting-data-files.html

default_logging:
Default: True
Sets up a basic logger on STDOUT

skip_checks:
Default: False
Skips integrity checks on the type, etc of the file being uploaded

skip_views:
Default: False
Does not attempt to save/reinstantiate view

load_as_csv:
Default: False
Tells the program that the input should be treated as a CSV and should not be converted to a DataFrame

no_header:
Default: False
Indicates the CSV has no header row at the beginning. The uploader will otherwise consider every row to be data
""".strip())