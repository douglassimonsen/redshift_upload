import pandas
from db_interfaces import redshift
import constants
import numpy
import utilities
import os
import shutil
import json


def load_source(source, source_args, source_kwargs):
    if isinstance(source, str):
        if source.endswith('.xlsx'):
            return pandas.read_excel(source, *source_args, **source_kwargs)
        if source.endswith(".csv"):
            return pandas.read_csv(source, *source_args, **source_kwargs)
    if isinstance(source, pandas.DataFrame):
        return source


def fix_column_types(df, predefined_columns):  # check what happens ot the dic over multiple uses
    def to_bool(col):
        assert col.replace({None: "nan"}).astype(str).str.lower().fillna("nan").isin(["true", "false", "nan"]).all()  # Nones get turned into nans and nans get stringified
        return col.replace({None: "nan"}).astype(str).str.lower().fillna("nan").apply(lambda x: str(x == "true") if x != "nan" else "")  # null is blank because the copy command defines it that way

    def bad_bool(col):
        bad_rows = col[~col.replace({None: "nan"}).astype(str).str.lower().isin(["true", "false", "nan"])].iloc[:5]
        constants.log.error(f"Column: {col.name} failed to be cast to bool")
        constants.log.error(f"The first 5 bad values are: {', '.join(str(x) for x in bad_rows.values)}")
        constants.log.error(f"The first 5 bad indices are: {', '.join(str(x) for x in bad_rows.index)}")

    def to_date(col):
        if pandas.isnull(col).all():  # pandas.to_datetime can fail on a fully empty column
            return col.fillna("")
        col = pandas.to_datetime(col)

        real_dates = col[~col.isna()]  # NA's don't behave well here
        assert (real_dates == pandas.to_datetime(real_dates.dt.date)).all()  # checks that there is no non-zero time component

        return col.dt.strftime(constants.DATE_FORMAT).replace({constants.NaT: "", "NaT": ""})

    def bad_date(col):
        mask1 = pandas.to_datetime(col, errors="coerce").isna()  # not even datetimes
        dts = col[~mask1]
        mask2 = dts != pandas.to_datetime(dts.dt.date)  # has non-zero time component
        bad_rows = col[mask1 | mask2].iloc[:5]
        constants.log.error(f"Column: {col.name} failed to be cast to date")
        constants.log.error(f"The first 5 bad values are: {', '.join(str(x) for x in bad_rows.values)}")
        constants.log.error(f"The first 5 bad indices are: {', '.join(str(x) for x in bad_rows.index)}")

    def to_dt(col):
        if pandas.isnull(col).all():  # pandas.to_datetime can fail on a fully empty column
            return col.fillna("")
        return pandas.to_datetime(col).dt.strftime(constants.DATETIME_FORMAT).replace({constants.NaT: "", "NaT": ""})

    def bad_dt(col):
        bad_rows = col[pandas.to_datetime(col, errors="coerce").isna()].iloc[:5]
        constants.log.error(f"Column: {col.name} failed to be cast to datetime")
        constants.log.error(f"The first 5 bad values are: {', '.join(str(x) for x in bad_rows.values)}")
        constants.log.error(f"The first 5 bad indices are: {', '.join(str(x) for x in bad_rows.index)}")

    def to_int(col):
        return col.astype("float64").astype("Int64")  # this float64 is necessary to cast columns like [1.0, "2", "3.0"] to [1, 2, 3]

    def bad_int(col):
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

        constants.log.error(f"Column: {col.name} failed to be cast to integer")
        constants.log.error(f"The first 5 bad values are: {', '.join(str(x) for x in bad_values)}")
        constants.log.error(f"The first 5 bad indices are: {', '.join(str(x) for x in bad_indices)}")

    def to_float(col):
        return col.astype("float64")

    def bad_float(col):
        bad_rows = col[pandas.to_numeric(col, errors="coerce").isna()].iloc[:5]
        constants.log.error(f"Column: {col.name} failed to be cast to datetime")
        constants.log.error(f"The first 5 bad values are: {', '.join(str(x) for x in bad_rows.values)}")
        constants.log.error(f"The first 5 bad indices are: {', '.join(str(x) for x in bad_rows.index)}")

    def to_string(col):
        return col.astype(str).replace({k: numpy.NaN for k in ["nan", "NaN", "None"]})

    def protect_colname(cols):
        ret_cols = []
        for c in cols:
            ret_cols.append(f'"{c}"')
        return ret_cols

    def clean_column(col, i, cols):
        col_count = cols[:i].to_list().count(col)
        if col_count != 0:
            col = f"{col}{col_count}"
        return col.replace(".", "_")[:constants.MAX_COLUMN_LENGTH]

    def try_types(col):
        for col_type, conv_func in [("boolean", to_bool), ("bigint", to_int), ("double precision", to_float), ("date", to_date), ("timestamp", to_dt)]:
            try:
                return col_type, conv_func(col)
            except:
                pass

        string_length = min(65535, max(20, 2 * col.astype(str).str.len().max()))
        return f"varchar({string_length})", to_string(col)

    def cast(col, col_type):
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

    df.columns = df.columns.str.lower()
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

        types.append(col_type)
    return df, types


def get_defined_columns(source, columns, interface, upload_options):
    def convert_column_type_structure(columns):
        for col, typ in columns.items():
            if not isinstance(typ, dict):
                columns[col] = {"type": typ}
        return columns

    columns = convert_column_type_structure(columns)
    if upload_options['drop_table'] is False:
        existing_columns = interface.get_columns()
    else:
        existing_columns = {}
    return {**columns, **existing_columns}  # we prioritize existing columns, since they are generally unfixable


def log_dependent_views(interface):
    def log_query(metadata):
        metadata["text"] = f"set search_path = '{interface.schema_name}';\nCREATE {metadata.get('view_type', 'view')} {metadata['view_name']} as\n{metadata['text']}"
        base_path = f"temp_view_folder/{interface.name}/{interface.table_name}"
        base_file = f"{base_path}/{metadata['view_name']}"
        os.makedirs(base_path, exist_ok=True)
        ages = ["_oldest", "_older", ""]

        for later_age, earlier_age in zip(ages[:-1], ages[1:]):
            if os.path.exists(f"{base_file}{earlier_age}.txt"):
                shutil.copy(f"{base_file}{earlier_age}.txt", f"{base_file}{later_age}.txt")

        with open(f"{base_file}.txt", "w") as f:
            json.dump(metadata, f)

    dependent_views = interface.get_dependent_views()
    with utilities.change_directory():
        for view_metadata in dependent_views:
            log_query(view_metadata)


def compare_with_remote(source_df, interface):
    remote_cols = interface.get_remote_cols()
    remote_cols_set = set(remote_cols)
    local_cols = set(source_df.columns.to_list())
    if not local_cols.issubset(remote_cols_set):  # means there are new columns in the local data
        missing_cols = ', '.join(local_cols.difference(remote_cols_set))
        raise ValueError(f"Haven't implemented adding new columns to the remote table yet. Bad columns are \"{missing_cols}\". Failing now")
    else:
        for col in remote_cols_set.difference(local_cols):
            source_df[col] = None
    source_df = source_df[remote_cols]


def upload(
    source=None,
    source_args=None,
    source_kwargs=None,
    column_types=None,
    schema_name=None,
    table_name=None,
    redshift_username=None,
    redshift_password=None,
    access_key=None,
    secret_key=None,
    upload_options={}
):

    UPLOAD_DEFAULTS = {
        "truncate_table": False,
        "drop_table": False,
        "cleanup_s3": False,
        "grant_access": [],
    }
    upload_options = {**UPLOAD_DEFAULTS, **upload_options}
    source_args = source_args or []
    source_kwargs = source_kwargs or {}
    column_types = column_types or {}

    interface = redshift.Interface(schema_name, table_name, redshift_username, redshift_password, access_key, secret_key)
    source = load_source(source, source_args, source_kwargs)

    column_types = get_defined_columns(source, column_types, interface, upload_options)
    source, column_types = fix_column_types(source, column_types)

    if not upload_options['drop_table']:
        compare_with_remote(source, interface)

    if upload_options['drop_table']:
        log_dependent_views(interface)

    interface.load_to_s3(source.to_csv(None, index=False, header=False))
    interface.get_exclusive_lock()
