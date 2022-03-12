import pandas  # type: ignore
from typing import Iterable, List, Dict, Tuple, Iterator, Any, Optional
import logging
import sys
import io
import csv
import math
import itertools
import collections
import colorama
import os

colorama.init()
import requests
import datetime
import getpass
import bz2


try:
    import constants, column_type_utilities  # type: ignore
    from db_interfaces import redshift  # type: ignore
except ModuleNotFoundError:
    from . import constants, column_type_utilities
    from .db_interfaces import redshift  # type: ignore
log = logging.getLogger("redshift_utilities")
csv_reader_type = type(
    csv.reader(io.StringIO())
)  # the actual type is trapped in a compiled binary. See more here: https://stackoverflow.com/questions/46673845/why-is-csv-reader-not-considered-a-class


class Source:
    """
    A class representing the data to be loaded to Redshift
    """

    def __init__(self, f: io.StringIO) -> None:
        f.seek(0, os.SEEK_END)
        self.size = f.tell()
        f.seek(0)
        dict_reader = csv.DictReader(f)
        self.source = f
        self.fieldnames = dict_reader.fieldnames or []
        self.num_rows = self._count_rows(dict_reader)
        self.predefined_columns: Dict = {}
        self.column_types: Dict = {}
        self.fixed_columns: List = []

    @staticmethod
    def _count_rows(iterable: Iterable) -> int:
        # This is 10-25% faster than len(list())
        # See: https://stackoverflow.com/questions/3345785/getting-number-of-elements-in-an-iterator-in-python
        counter = itertools.count()
        collections.deque(zip(iterable, counter), maxlen=0)
        return next(counter)

    def dictrows(self) -> csv.DictReader:
        self.source.seek(0)
        return csv.DictReader(self.source)

    def rows(self) -> Iterable:
        self.source.seek(0)
        return csv.reader(self.source)


class CustomFormatter(logging.Formatter):
    FORMAT_STR = "%(asctime)s - %(levelname)s: %(message)s (%(filename)s:%(lineno)d)"
    FORMATS = {
        logging.DEBUG: colorama.Fore.BLUE,
        logging.INFO: colorama.Fore.GREEN,
        logging.WARNING: colorama.Fore.YELLOW,
        logging.ERROR: colorama.Fore.RED,
        logging.CRITICAL: colorama.Fore.RED,
    }

    def format(self, record: logging.LogRecord) -> str:
        log_fmt = (
            self.FORMATS[record.levelno] + self.FORMAT_STR + colorama.Style.RESET_ALL
        )
        formatter = logging.Formatter(log_fmt, datefmt="%Y-%m-%d %H:%M:%S")
        return formatter.format(record)


def initialize_logger(log_level: str) -> None:
    """
    Sets up logging for the upload
    """
    log = logging.getLogger("redshift_utilities")
    log.setLevel(logging.getLevelName(log_level))
    if log.hasHandlers():
        return
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(CustomFormatter())
    log.addHandler(handler)


def chunkify(source: Source, upload_options: Dict) -> Tuple[List[bytes], int]:
    """
    Breaks the single file into multiple smaller chunks to speed loading into S3 and copying into Redshift
    """

    def ideal_load_count() -> int:
        if upload_options["load_in_parallel"]:
            return upload_options["load_in_parallel"]
        # sort of arbitrary tbh. Really should be breaking it into 1GB chunks, but don't know how to do that efficiently & in parallel

        # https://docs.aws.amazon.com/redshift/latest/dg/c_best-practices-use-multiple-files.html
        # We're dividing by 70% because the BZIP2 seems to compress things by 30% and we are sending the compressed blobs, but currently working with the uncompressed data
        min_s3_size = 1 * 1024 ** 2 / 0.7  # 1   MB
        max_s3_size = 125 * 1024 ** 2 / 0.7  # 125 BB

        min_slices = int(source.size // max_s3_size)
        max_slices = int(source.size // min_s3_size)

        # Slicing protocol:
        # Suppose we have a file of n bytes
        # 1. If we have more than max_s3_size of data, we split it into the largest of:
        #   a. the greatest multiple of node_count less than (n // max_s3_size)
        #   b. node_count
        # 2. If we have more than min_s3_size of data, we split it into the largest of:
        #   a. the greatest multiple of node_count less than (n // min_s3_size)
        #   b. node_count
        # 3. Otherwise, we have a single chunk

        if min_slices > 0:
            min_slices -= min_slices % upload_options["node_count"]
            return max(min_slices, upload_options["node_count"])
        elif max_slices > 0:
            max_slices -= max_slices % upload_options["node_count"]
            return max(max_slices, upload_options["node_count"])
        else:
            return 1

    def chunk_to_string(chunk: List[str]) -> bytes:
        buffer = io.StringIO()
        writer = csv.writer(buffer)
        writer.writerows(chunk)
        buffer.seek(0)
        compressed = bz2.compress(buffer.read().encode("utf-8"))
        return compressed

    col_conversions = [
        col.get("converter_func", lambda x: x) for col in source.column_types.values()
    ]
    rows = list(source.rows())[1:]  # the first is the header
    rows = [
        [func(x) for func, x in zip(col_conversions, row)] for row in rows
    ]  # currently forcing 1.0, 2.0 -> 1, 2 and "true", "1" -> True
    load_in_parallel = ideal_load_count()
    chunk_size = math.ceil(source.num_rows / load_in_parallel)
    return [
        chunk_to_string(rows[offset : (offset + chunk_size)])  # noqa
        for offset in range(0, source.num_rows, chunk_size)
    ], load_in_parallel


def load_source(source: constants.SourceOptions, upload_options: Dict = None) -> Source:
    """
    Loads/transforms the source data to simplify data handling for the rest of the program.
    Accepts a DataFrame, a csv.reader, a list, or a path to a csv/xlsx file.
    source_args and source_kwargs both get passed to the csv.reader, pandas.read_excel, and pandas.read_csv functions
    """
    if upload_options is None:
        upload_options = constants.UPLOAD_DEFAULTS

    for key in upload_options.keys():
        if key not in constants.UPLOAD_DEFAULTS:
            import fuzzywuzzy.process

            probable_keys = fuzzywuzzy.process.extract(
                key, constants.UPLOAD_DEFAULTS.keys(), limit=2
            )
            probable_keys = [
                x[0] for i, x in enumerate(probable_keys) if x[1] >= 80 or i == 0
            ]
            if len(probable_keys) == 2:
                raise ValueError(
                    f"Key '{key}' not a valid upload option. Do you mean '{probable_keys[0]}' or '{probable_keys[1]}'?"
                )
            if len(probable_keys) == 1:
                raise ValueError(
                    f"Key '{key}' not a valid upload option. Do you mean '{probable_keys[0]}'?"
                )

    if isinstance(
        source, bytes
    ):  # we're assuming it's utf-8 for now, but it should be converted into an upload_option later
        source = source.decode("utf-8")

    if isinstance(
        source, (io.StringIO, io.TextIOWrapper)
    ):  # the second is the type of open(x, 'r')
        return Source(source)

    elif isinstance(source, str):
        log.debug(
            "If you have a CSV that happens to end with .csv, this will treat it as a path. This is a reason all files ought to end with a newline"
        )
        if source.endswith(".csv"):
            f_in = open(source, "r")
            if upload_options["stream_from_file"]:
                return Source(f_in)
            else:
                f_out = io.StringIO()  # we need to load the file in memory
                f_out.write(f_in.read())
                f_in.close()
                return Source(f_out)

        else:
            f = io.StringIO()
            f.write(source)
            return Source(f)

    elif isinstance(source, list):
        if len(source) == 0:
            raise ValueError("We cannot accept empty lists as a source")
        f = io.StringIO()
        dict_writer = csv.DictWriter(f, source[0].keys())
        dict_writer.writeheader()
        dict_writer.writerows(source)
        return Source(f)

    elif isinstance(source, pandas.DataFrame):
        f = io.StringIO()
        source.to_csv(f, index=False)
        return Source(f)

    raise ValueError("We do not support this type of source")


def get_bad_vals(rows: Iterator[Dict], col: str, type_info: Dict, top: int = 5):
    """
    An error logging function to identify the first n values in a column that don't match the predefined type of the column
    """
    bad_vals = []
    bad_indices = []
    for i, row in enumerate(rows):
        if not type_info["func"](row[col], None):
            bad_vals.append(row[col])
            bad_indices.append(str(i))
            if len(bad_vals) == top:
                break
    log.error(
        f"We are showing the top {len(bad_vals)} non-conforming values for column \"{col}\" (type: {type_info['type']})"
    )
    log.error(f"The improper values are: {', '.join(bad_vals)}")
    log.error(f"The improper rows are: {', '.join(bad_indices)}")
    if len(bad_vals) < top:
        log.error("There are no other bad values for this column")


def fix_column_types(
    source: Source, interface: redshift.Interface, drop_table: bool
) -> None:  # check what happens to the dict over multiple uses
    """
    Verifies the column names are not too long.
    Verifies the column data matches any predefined types.
    Generates an appropriate type for undefined columns.
    If varchars are longer than acceptable for the remote, expands the column
    """

    def clean_column(col: str, i: int, cols: List[str]) -> str:
        col_count = cols[:i].count(col)
        if col_count != 0:
            col = f"{col}{col_count + 1}"
        return col.replace(".", "_")[
            : constants.MAX_COLUMN_LENGTH
        ]  # yes, this could cause a collision, but probs not

    log.info("Determining proper column types for serialization")
    fixed_columns = [
        x.lower() for x in source.fieldnames
    ]  # need to lower everyone first, before checking for dups
    source.fixed_columns = [
        clean_column(x, i, fixed_columns) for i, x in enumerate(fixed_columns)
    ]
    col_types = {
        col: column_type_utilities.get_possible_data_types()
        for col in source.fieldnames
    }
    for col, col_info in source.predefined_columns.items():
        if col in col_types:
            col_types[col] = [
                x for x in col_types[col] if x["type"] == col_info["type"]
            ]

    non_viable_cols = []
    for row in source.dictrows():
        for col, data in col_types.items():
            viable_types = [x for x in data if x["func"](row[col], x)]
            if (
                not viable_types
            ):  # means that each one failed to parse at least one entry
                if (
                    col in source.predefined_columns
                ):  # means that the new data doesn't match the old
                    get_bad_vals(
                        source.dictrows(),
                        col,
                        [
                            x
                            for x in column_type_utilities.get_possible_data_types()
                            if x["type"] == source.predefined_columns[col]["type"]
                        ][0],
                    )  # TODO: iterate over rows just once, rather than once per bad col
                non_viable_cols.append(col)
            col_types[col] = viable_types

    if non_viable_cols:
        log.error(
            f"The following columns could not be parsed: {', '.join(non_viable_cols)}. Aborting now"
        )
        raise ValueError("Some columns could not match to a valid Redshift column type")

    source.column_types = {
        k: v[0] for k, v in col_types.items()
    }  # we want the most specialized possible type for each column
    for colname, col_info in source.column_types.items():
        if col_info["type"] in ("SMALLINT", "INTEGER", "BIGINT"):
            col_info["converter_func"] = lambda x: int(float(x)) if x != "" else None
        if col_info["type"] == "BOOLEAN":
            col_info["converter_func"] = (
                lambda x: str(x).lower() in ("1", "true") if x != "" else None
            )
        elif (
            col_info["type"] == "VARCHAR"
            and interface.table_exists
            and not drop_table
            and colname in source.predefined_columns
        ):  # these conditions mean we have data that will be too large for the data
            if col_info["suffix"] > source.predefined_columns[colname]["suffix"]:
                if not interface.expand_varchar_column(colname, col_info["suffix"]):
                    log.error(
                        f"Unable to load data to table: {interface.full_table_name}"
                    )
                    raise ValueError(
                        "Failed to expand the varchar column enough to accomodate the new data."
                    )


def check_coherence(
    schema_name: str,
    table_name: str,
    upload_options: Optional[Dict],
    aws_info: Optional[Dict],
) -> Tuple[Dict[str, Any], Dict[str, Any]]:
    """
    Checks the upload_options dictionary for incompatible selections. Current incompatible options:

    If a distkey or sortkey is set, the diststyle will be set to key (https://docs.aws.amazon.com/redshift/latest/dg/c_choosing_dist_sort.html)
    load_in_parallel must be an integer
    Both schema_name and table_name must be set
    At most one of truncate_table and drop_table can be set to True
    redshift_username, redshift_password, access_key, secret_key, bucket, host, dbname, port must all be set
    You cannot both skip_checks and drop_table, since we need to calculate the column types when recreating the table. Note: if skip_checks is True and the table doesn't exist yet, the program will raise a ValueError when it checks for the table's existence
    """
    upload_options = {**constants.UPLOAD_DEFAULTS, **(upload_options or {})}
    aws_info = aws_info or {}

    if upload_options["distkey"] or upload_options["sortkey"]:
        upload_options["diststyle"] = "key"

    if upload_options["load_in_parallel"] is not None and not isinstance(
        upload_options["load_in_parallel"], int
    ):
        raise ValueError("The option load_in_parallel must be an integer")

    if not schema_name or not table_name:
        raise ValueError("You need to define the name of the table you want to load to")

    if (
        upload_options["truncate_table"] is True
        and upload_options["drop_table"] is True
    ):
        raise ValueError("You must only choose one. It doesn't make sense to do both")

    for section, params in {
        "db": ["user", "password", "host", "dbname", "port"],
        "s3": ["access_key", "secret_key"],
        "constants": ["bucket"],  # default_schema can be not set
    }.items():
        for c in params:
            if not aws_info.get(section, {}).get(c):  # can't be null or empty strings
                raise ValueError(f"You need to define {c} in the aws_info dictionary")

    if upload_options["skip_checks"] and upload_options["drop_table"]:
        raise ValueError(
            "If you're dropping the table, you need the checks to determine what column types to use"
        )

    for c in ["default_timeout", "lock_timeout"]:
        if not isinstance(upload_options[c], int):
            raise ValueError(
                f'{c} must be an int. Currently it is set to "{upload_options[c]}" (type: {type(upload_options[c]).__name__})'
            )
    return upload_options, aws_info


def post_data(
    endpoint: str,
    endpoint_type: str,
    interface: redshift.Interface,
    load_duration: float,
    source: Source,
) -> None:
    """
    Records basic information about the upload session to a table.
    Happens at the end so a failure here won't impact the overall upload.
    """
    log.info("Recording Redshift Upload")
    data = {
        "db_name": interface.aws_info["db"]["dbname"],
        "schema_name": interface.schema_name,
        "table_name": interface.table_name,
        "load_duration": load_duration,
        "completed_dt": datetime.datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
        "rows": source.num_rows,
        "redshift_user": interface.aws_info["db"]["user"],
        "os_user": getpass.getuser(),
    }
    query = f"""
    insert into {endpoint} ({", ".join(data.keys())})
    values ({", ".join(f"%({x})s" for x in data.keys())})
    """
    if endpoint_type == "api":
        requests.post(endpoint, json=data)
    elif endpoint_type == "db":
        conn = interface.get_db_conn()
        cursor = conn.cursor()
        cursor.execute(query, data)
        conn.commit()
