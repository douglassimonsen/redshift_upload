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
    import constants
    from db_interfaces import redshift
except ModuleNotFoundError:
    from . import constants
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
    def chunk_to_string(chunk):
        f = io.StringIO()
        writer = csv.writer(f)
        writer.writerows(chunk)
        f.seek(0)
        return f.read().encode("utf-8")

    if not upload_options['load_as_csv']:
        load_in_parallel = min(upload_options['load_in_parallel'], source.shape[0])  # cannot have more groups than rows, otherwise it breaks
        if load_in_parallel > 1:
            chunk_size = math.ceil(source.shape[0] / load_in_parallel)
            chunks = numpy.arange(source.shape[0]) // chunk_size
            return [chunk.to_csv(None, index=False, header=False, encoding="utf-8") for _, chunk in source.groupby(chunks)], load_in_parallel
        else:
            return [source.to_csv(None, index=False, header=False, encoding="utf-8")], load_in_parallel
    else:
        rows = list(source)
        if not upload_options['no_header']:  # we can't include the header row, Redshift will treat it like a normal row (and this is easier than giving every parallel a header and using the IGNOREHEADER option in the copy command)
            rows = rows[1:]
        load_in_parallel = min(upload_options['load_in_parallel'], len(rows))  # cannot have more groups than rows, otherwise it breaks
        chunk_size = math.ceil(len(rows) / load_in_parallel)
        return [chunk_to_string(rows[offset:(offset + chunk_size)]) for offset in range(0, len(rows), chunk_size)], load_in_parallel


def load_source(source: constants.SourceOptions, source_args: List, source_kwargs: Dict, upload_options: Dict) -> Union[pandas.DataFrame, csv.reader]:
    if upload_options['load_as_csv']:
        if isinstance(source, csv_reader_type):
            return source
        if source.endswith(".csv"):
            log.debug("If you have a CSV that happens to end with .csv, this will treat it as a path. This is a reason all files ought to end with a newline")
            log.debug("Also, if you do not have a header row, you need to set 'header_row' = False")
            f_in_mem = io.StringIO()  # we need to load the file in memory
            with open(source, 'r') as f:
                f_in_mem.write(f.read())
            f_in_mem.seek(0)
            return csv.reader(f_in_mem, *source_args, **source_kwargs)
        else:
            if isinstance(source, bytes):
                source = source.decode("utf-8")
            return csv.reader(io.StringIO(source), *source_args, **source_kwargs)
    elif isinstance(source, str):
        if source.endswith('.xlsx'):
            return pandas.read_excel(source, *source_args, **source_kwargs)
        elif source.endswith(".csv"):
            return pandas.read_csv(source, *source_args, **source_kwargs)
        else:
            raise ValueError("Your input was invalid")
    elif isinstance(source, pandas.DataFrame):
        return source
    elif isinstance(source, list):
        if len(source) == 0:
            ValueError("We do not accept lists of zero length")
        else:
            return pandas.DataFrame(source)
    else:
        raise ValueError("We do not support this type of source")


def fix_column_types(df: pandas.DataFrame, predefined_columns: Dict, interface: redshift.Interface, drop_table: bool) -> Tuple[pandas.DataFrame, Dict]:  # check what happens ot the dic over multiple uses
    def to_bool(col: pandas.Series):
        assert col.replace({None: "nan"}).astype(str).str.lower().fillna("nan").isin(["true", "false", "nan"]).all()  # Nones get turned into nans and nans get stringified
        return col.replace({None: "nan"}).astype(str).str.lower().fillna("nan").apply(lambda x: str(x == "true") if x != "nan" else "")  # null is blank because the copy command defines it that way

    def bad_bool(col: pandas.Series):
        bad_rows = col[~col.replace({None: "nan"}).astype(str).str.lower().isin(["true", "false", "nan"])].iloc[:5]
        log.error(f"Column: {col.name} failed to be cast to bool")
        log.error(f"The first 5 bad values are: {', '.join(str(x) for x in bad_rows.values)}")
        log.error(f"The first 5 bad indices are: {', '.join(str(x) for x in bad_rows.index)}")

    def to_date(col: pandas.Series):
        if pandas.isnull(col).all():  # pandas.to_datetime can fail on a fully empty column
            return col.fillna("")
        col = pandas.to_datetime(col)

        real_dates = col[~col.isna()]  # NA's don't behave well here
        assert (real_dates == pandas.to_datetime(real_dates.dt.date)).all()  # checks that there is no non-zero time component

        return col.dt.strftime(constants.DATE_FORMAT).replace({constants.NaT: "", "NaT": ""})

    def bad_date(col: pandas.Series):
        mask1 = pandas.to_datetime(col, errors="coerce").isna()  # not even datetimes
        dts = col[~mask1]
        mask2 = dts != pandas.to_datetime(dts.dt.date)  # has non-zero time component
        bad_rows = col[mask1 | mask2].iloc[:5]
        log.error(f"Column: {col.name} failed to be cast to date")
        log.error(f"The first 5 bad values are: {', '.join(str(x) for x in bad_rows.values)}")
        log.error(f"The first 5 bad indices are: {', '.join(str(x) for x in bad_rows.index)}")

    def to_dt(col: pandas.Series):
        if pandas.isnull(col).all():  # pandas.to_datetime can fail on a fully empty column
            return col.fillna("")
        return pandas.to_datetime(col).dt.strftime(constants.DATETIME_FORMAT).replace({constants.NaT: "", "NaT": ""})

    def bad_dt(col: pandas.Series):
        bad_rows = col[pandas.to_datetime(col, errors="coerce").isna()].iloc[:5]
        log.error(f"Column: {col.name} failed to be cast to datetime")
        log.error(f"The first 5 bad values are: {', '.join(str(x) for x in bad_rows.values)}")
        log.error(f"The first 5 bad indices are: {', '.join(str(x) for x in bad_rows.index)}")

    def to_int(col: pandas.Series):
        return col.astype("float64").astype("Int64")  # this float64 is necessary to cast columns like [1.0, "2", "3.0"] to [1, 2, 3]

    def bad_int(col: pandas.Series):
        """
        This has been designed to match the functions

        safe_cast (line 135)
        coerce_to_array (line 155, specifically area 206-213)
        in
        pandas/core/arrays/integer.py
        """

        dtyp = pandas.api.types.infer_dtype(col)
        acceptable_types = (
            "floating",
            "integer",
            "mixed-integer",
            "integer-na",
            "mixed-integer-float",
        )
        if dtyp not in acceptable_types:  # probably a string
            bad_indices = []
            bad_values = []
            for i, e in zip(col.index, col.values):
                if pandas.api.types.infer_dtype([e]) not in acceptable_types:
                    bad_indices.append(i)
                    bad_values.append(e)
                    if len(bad_indices) == 5:
                        break
        else:  # probably a float that isn't representing a integer (like 1.1 vs 1.0)
            bad_rows = col[(col.values.astype("int64", copy=True) != col.values)].iloc[:5]
            bad_indices = bad_rows.index
            bad_values = bad_rows.values

        log.error(f"Column: {col.name} failed to be cast to integer")
        log.error(f"The first 5 bad values are: {', '.join(str(x) for x in bad_values)}")
        log.error(f"The first 5 bad indices are: {', '.join(str(x) for x in bad_indices)}")

    def to_float(col: pandas.Series):
        return col.astype("float64")

    def bad_float(col: pandas.Series):
        bad_rows = col[pandas.to_numeric(col, errors="coerce").isna()].iloc[:5]
        log.error(f"Column: {col.name} failed to be cast to datetime")
        log.error(f"The first 5 bad values are: {', '.join(str(x) for x in bad_rows.values)}")
        log.error(f"The first 5 bad indices are: {', '.join(str(x) for x in bad_rows.index)}")

    def to_string(col: pandas.Series):
        return col.astype(str).replace({k: numpy.NaN for k in ["nan", "NaN", "None"]})

    def clean_column(col: pandas.Series, i: int, cols: pandas.Series):
        col_count = cols[:i].to_list().count(col)
        if col_count != 0:
            col = f"{col}{col_count}"
        return col.replace(".", "_")[:constants.MAX_COLUMN_LENGTH]

    def try_types(col: pandas.Series):
        for col_type, conv_func in [("boolean", to_bool), ("bigint", to_int), ("double precision", to_float), ("date", to_date), ("timestamp", to_dt)]:
            try:
                return col_type, conv_func(col)
            except:
                pass

        string_length = max(1, min(constants.MAX_VARCHAR_LENGTH, col.astype(str).str.encode("utf-8").str.len().max()))  # necessary to handle emojis, since len('Aݔ') is 2, but it contains 3 bytes which is what Redshift cares about
        return f"varchar({string_length})", to_string(col)

    def cast(col: pandas.Series, col_type: str):
        col_type = col_type.lower()
        col_conv = {
            "boolean": to_bool,
            "bigint": to_int,
            "date": to_date,
            "double precision": to_float,
            "timestamp": to_dt,
        }.get(col_type, to_string)
        bad_conv = {
            "boolean": bad_bool,
            "bigint": bad_int,
            "date": bad_date,
            "double precision": bad_float,
            "timestamp": bad_dt,
        }  # we're not including strings, how can strings fail (says man about to observe just that...)
        try:
            return col_conv(col)
        except:
            bad_conv[col_type](col)
            raise BaseException

    log.info("Determining proper column types for serialization")
    df.columns = df.columns.astype(str).str.lower()
    df.columns = [clean_column(x, i, df.columns) for i, x in enumerate(df.columns)]
    types = []
    for colname in df.columns:
        if colname in predefined_columns:
            col_type = predefined_columns[colname]["type"]
            df[colname] = cast(df[colname], col_type)

        else:
            col = df[colname]
            if col.dtype.name in constants.DTYPE_MAPS:
                col_type = constants.DTYPE_MAPS[col.dtype.name]
            else:
                col_type, col_cast = try_types(col)
                df[colname] = col_cast

        if col_type.startswith("varchar") and interface.table_exists and not drop_table:
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
    return df, dict(zip(df.columns, types))


def check_coherence(schema_name: str, table_name: str, upload_options: Dict, aws_info: Dict) -> Tuple[Dict, Dict]:
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
