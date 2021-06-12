import pandas
from typing import List, Dict, Union, Tuple
import numpy
import logging
import re
import sys
import io
import csv
import math
try:
    import constants, column_type_utilities
    from db_interfaces import redshift
except ModuleNotFoundError:
    from . import constants, column_type_utilities
    from .db_interfaces import redshift
log = logging.getLogger("redshift_utilities")
csv_reader_type = type(csv.reader(io.StringIO()))  # the actual type is trapped in a compiled binary. See more here: https://stackoverflow.com/questions/46673845/why-is-csv-reader-not-considered-a-class


def initialize_logger(log_level) -> None:
    """
    Sets up logging for the upload
    """
    log = logging.getLogger("redshift_utilities")
    log.setLevel(logging.getLevelName(log_level))
    if log.hasHandlers():
        return
    handler = logging.StreamHandler(sys.stdout)
    formatter = logging.Formatter("%(asctime)s - %(levelname)s: %(message)s", datefmt="%Y-%m-%d %H:%M:%S")
    handler.setFormatter(formatter)
    log.addHandler(handler)


def chunkify(source, upload_options) -> List[str]:
    """
    Breaks the single file into multiple smaller chunks to speed loading into S3 and copying into Redshift
    """
    def chunk_to_string(chunk):
        f = io.StringIO()
        writer = csv.writer(f)
        writer.writerows(chunk)
        f.seek(0)
        return f.read().encode("utf-8")

    rows = list(csv.reader(source))[1:]
    load_in_parallel = min(upload_options['load_in_parallel'], len(rows))  # cannot have more groups than rows, otherwise it breaks
    chunk_size = math.ceil(len(rows) / load_in_parallel)
    return [chunk_to_string(rows[offset:(offset + chunk_size)]) for offset in range(0, len(rows), chunk_size)], load_in_parallel


def load_source(source: constants.SourceOptions) -> Union[pandas.DataFrame, csv.reader]:
    """
    Loads/transforms the source data to simplify data handling for the rest of the program.
    Accepts a DataFrame, a csv.reader, a list, or a path to a csv/xlsx file.
    source_args and source_kwargs both get passed to the csv.reader, pandas.read_excel, and pandas.read_csv functions
    """
    if isinstance(source, csv_reader_type):
            return list(source)

    elif isinstance(source, str):
        log.debug("If you have a CSV that happens to end with .csv, this will treat it as a path. This is a reason all files ought to end with a newline")
        log.debug("Also, if you do not have a header row, you need to set 'header_row' = False")
        if source.endswith(".csv"):
            f = io.StringIO()  # we need to load the file in memory
            with open(source, 'r') as f:
                f.write(f.read())
            f.seek(0)
            return f

        else:
            if isinstance(source, bytes):
                source = source.decode("utf-8")
            f = io.StringIO()
            f.write(source)
            f.seek(0)
            return f

    elif isinstance(source, list):
        if len(source) == 0:
            raise ValueError("We cannot except empty lists as a source")
        f = io.StringIO()
        with open(f, 'w', newline='') as output_file:
            dict_writer = csv.DictWriter(output_file, source[0].keys())
            dict_writer.writeheader()
            dict_writer.writerows(source)
        f.seek(0)
        return f

    elif isinstance(source, pandas.DataFrame):
        f = io.StringIO()
        source.to_csv(f, index=False)
        f.seek(0)
        return f

    raise ValueError("We do not support this type of source")


def fix_column_types(df: pandas.DataFrame, predefined_columns: Dict, interface: redshift.Interface, drop_table: bool) -> Tuple[pandas.DataFrame, Dict]:  # check what happens ot the dic over multiple uses
    def clean_column(col: pandas.Series, i: int, cols: List):
        col_count = cols[:i].count(col)
        if col_count != 0:
            col = f"{col}{col_count}"
        return col.replace(".", "_")[:constants.MAX_COLUMN_LENGTH]  # yes, this could cause a collision, but probs not

    log.info("Determining proper column types for serialization")
    columns = csv.DictReader(df).fieldnames; df.seek(0)
    fixed_columns = [x.lower() for x in columns]  # need to lower everyone first, before checking for dups
    fixed_columns = [clean_column(x, i, columns) for i, x in enumerate(columns)]
    col_types = {
        col: column_type_utilities.get_possible_data_types()
        for col in columns
    }

    for row in csv.DictReader(df):
        for col, data in col_types.items():
            viable_types = [x for x in data if x[1](row[col])]
            if not viable_types:
                raise ValueError("There are no valid types (not even VARCHAR) for this function!")
            col_types[col] = viable_types
    df.seek(0)

    col_types = {k: v[0] for k, v in col_types.items()}
    for colname in columns:
        continue
        if col_type.startswith("VARCHAR") and interface.table_exists and not drop_table:
            remote_varchar_length = int(re.search(constants.varchar_len_re, col_type).group(1))  # type: ignore
            bad_strings = df[colname][df[colname].astype(str).str.len() > remote_varchar_length]
            bad_strings_formatted = "\n".join(f"{x[:200]} <- (length: {len(str(x))}, index: {i})" for x, i in zip(bad_strings.iloc[:5], bad_strings.iloc[:5].index))
            max_str_len = max(bad_strings.astype(str).str.len(), default=-1)
            if bad_strings.shape[0] > 0:
                if not interface.expand_varchar_column(colname, max_str_len):
                    log.error(f"Unable to load data to table: {interface.full_table_name}")
                    log.error(f'The "{colname}" column had {bad_strings.shape[0]} string(s) longer than the table can support (max length: {max_str_len})')
                    log.error("These are the first five offending strings:\n" + bad_strings_formatted)
                    raise ValueError("Failed to expand the varchar column enough to accomodate the new data.")
                else:
                    col_type = re.sub(constants.varchar_len_re, f"({max_str_len})", col_type, count=1)
        types.append(col_type)
    return df, col_types, fixed_columns


def check_coherence(schema_name: str, table_name: str, upload_options: Dict, aws_info: Dict) -> Tuple[Dict, Dict]:
    """
    Checks the upload_options dictionary for incompatible selections. Current incompatible options:

    If a distkey or sortkey is set, the diststyle will be set to key (https://docs.aws.amazon.com/redshift/latest/dg/c_choosing_dist_sort.html)
    If load_as_csv is True, the program cannot check types (type checking is based off a pandas Dataframe) so skip_checks will be set to True
    If no_header is True and load_as_csv is False, we raise a ValueError because no_header is only used for CSVs
    load_in_parallel must be an integer
    Both schema_name and table_name must be set
    At most one of truncate_table and drop_table can be set to True
    redshift_username, redshift_password, access_key, secret_key, bucket, host, dbname, port must all be set
    You cannot both skip_checks and drop_table, since we need to calculate the column types when recreating the table. Note: if skip_checks is True and the table doesn't exist yet, the program will raise a ValueError when it checks for the table's existence
    """
    upload_options = {**constants.UPLOAD_DEFAULTS, **(upload_options or {})}
    aws_info = aws_info or {}
    if upload_options['distkey'] or upload_options['sortkey']:
        upload_options['diststyle'] = 'key'

    if upload_options['load_as_csv']:
        upload_options['skip_checks'] = True

    if upload_options['no_header'] and not upload_options['load_as_csv']:
        raise ValueError("This parameter is only used when using a CSV to upload")

    if not isinstance(upload_options['load_in_parallel'], int):
        raise ValueError("The option load_in_parallel must be an integer")

    if not schema_name or not table_name:
        raise ValueError("You need to define the name of the table you want to load to")

    if upload_options['truncate_table'] is True and upload_options['drop_table'] is True:
        raise ValueError("You must only choose one. It doesn't make sense to do both")

    for c in ["redshift_username", "redshift_password", "access_key", "secret_key", "bucket", "host", "dbname", "port"]:
        if not aws_info.get(c):  # can't be null or empty strings
            raise ValueError(f"You need to define {c} in the aws_info dictionary")

    if upload_options['skip_checks'] and upload_options['drop_table']:
        raise ValueError("If you're dropping the table, you need the checks to determine what column types to use")
    return upload_options, aws_info
