import pandas
from db_interfaces import redshift
import constants
import numpy
import utilities
import os
import json
import datetime
import psycopg2
import getpass
import re
import toposort
import logging
from typing import Dict, List, Union
log = logging.getLogger("redshift_utilities")
SourceOptions = Union[str, pandas.DataFrame, List[Dict]]

def load_source(source: SourceOptions, source_args: List, source_kwargs: Dict):
    if isinstance(source, str):
        if source.endswith('.xlsx'):
            return pandas.read_excel(source, *source_args, **source_kwargs)
        if source.endswith(".csv"):
            return pandas.read_csv(source, *source_args, **source_kwargs)
    if isinstance(source, pandas.DataFrame):
        return source
    else:
        return pandas.DataFrame(source)  # please don't do this


def fix_column_types(df: pandas.DataFrame, predefined_columns: Dict, interface: redshift.Interface, drop_table: bool):  # check what happens ot the dic over multiple uses
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

    def protect_colname(cols):
        ret_cols = []
        for c in cols:
            ret_cols.append(f'"{c}"')
        return ret_cols

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

        string_length = min(65535, col.astype(str).str.encode("utf-8").str.len().max())  # necessary to handle emojis, since len('Aݔ') is 2, but it contains 3 bytes which is what Redshift cares about
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

        if col_type.startswith("varchar") and interface.table_exists and not drop_table:
            remote_varchar_length = int(re.search(constants.varchar_len_re, col_type).group(1))  # type: ignore
            bad_strings = df[colname][df[colname].astype(str).str.len() > remote_varchar_length]
            bad_strings_formatted = "\n".join(f"{x} <- (length: {len(str(x))}, index: {i})" for x, i in zip(bad_strings, bad_strings.index))
            max_str_len = max(bad_strings.astype(str).str.len(), default=-1)
            if bad_strings.shape[0] > 0:
                if not interface.expand_varchar_column(colname, max_str_len):
                    raise ValueError("Failed to expand the varchar column enough to accomodate the new data.")
                else:
                    col_type = re.sub(constants.varchar_len_re, f"({max_str_len})", col_type, count=1)
        types.append(col_type)
    return df, dict(zip(df.columns, types))


def get_defined_columns(source: pandas.DataFrame, columns: Dict, interface: redshift.Interface, upload_options: Dict):
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


def log_dependent_views(interface: redshift.Interface):
    def log_query(metadata: Dict):
        metadata["text"] = f"set search_path = '{interface.schema_name}';\nCREATE {metadata.get('view_type', 'view')} {metadata['view_name']} as\n{metadata['text']}"
        base_path = f"temp_view_folder/{interface.name}/{interface.table_name}"
        base_file = f"{base_path}/{metadata['view_name']}"
        os.makedirs(base_path, exist_ok=True)

        with open(f"{base_file}.txt", "w") as f:
            json.dump(metadata, f)

    dependent_views = interface.get_dependent_views()
    with utilities.change_directory():
        for view_metadata in dependent_views:
            log_query(view_metadata)


def compare_with_remote(source_df: pandas.DataFrame, interface: redshift.Interface):
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


def s3_to_redshift(interface: redshift.Interface, column_types: Dict, upload_options: Dict):
    def delete_table():
        cursor.execute(f'drop table if exists {interface.full_table_name} cascade')

    def truncate_table():
        cursor.execute(f'truncate {interface.full_table_name}')

    def create_table():
        columns = ', '.join(f'"{k}" {v}' for k, v in column_types.items())
        cursor.execute(f'create table if not exists {interface.full_table_name} ({columns}) diststyle even')

    def grant_access():
        grant = f"GRANT SELECT ON {interface.full_table_name} TO {', '.join(upload_options['grant_access'])}"
        cursor.execute(grant)

    conn, cursor = interface.get_exclusive_lock()

    if upload_options['drop_table']:
        delete_table()
        create_table()
    if upload_options['truncate_table']:
        truncate_table()

    interface.copy_table(cursor)

    if upload_options['grant_access']:
        grant_access()

    conn.commit()

    if upload_options['cleanup_s3']:
        interface.delete_s3_object()


def reinstantiate_views(interface: redshift.Interface, drop_table: bool, grant_access: List):
    def gen_order(views: Dict):
        base_table = set([interface.full_table_name])
        dependencies = {}
        for view in views.values():
            dependencies[view['view_name']] = set(view['dependencies']) - base_table
        return toposort.toposort_flatten(dependencies)

    age_limit = datetime.datetime.today() - pandas.Timedelta(hours=4)
    views = {}
    base_path = f"temp_view_folder/{interface.name}/{interface.table_name}"
    with utilities.change_directory():
        possible_views = [os.path.join(base_path, view) for view in os.listdir(base_path) if view.endswith(".txt")]  # stupid thumbs.db ruining my life
        for f in possible_views:
            if datetime.datetime.fromtimestamp(os.path.getmtime(f)) > age_limit:
                with open(f, "r") as fl:
                    view_info = json.load(fl)
                views[view_info['view_name']] = view_info

    reload_order = gen_order(views)

    conn = interface.get_db_conn()
    cursor = conn.cursor()
    for view_name in reload_order:
        view = views[view_name]
        try:
            if drop_table is True:
                cursor.execute(view["text"])
                if grant_access:
                    cursor.execute(f'GRANT ALL ON {view["view_name"]} TO {", ".join(grant_access)}')
            elif view.get("view_type", "view") == "view":  # if there isn't a drop_table, the views still exist and we don't need to do anything
                pass
            else:  # only get here when complete_refresh is False and view_type is materialized view
                cursor.execute(f"refresh materialized view {view['view_name']}")
                cursor.close()
            conn.commit()
            os.remove(os.path.join(base_path, view["view_name"]) + ".txt")
        except psycopg2.ProgrammingError as e:  # if the type of column changed, a view can disapper.
            conn.rollback()
            print(f"We were unable to load view: {view_name}")
            print(f"You can see the view body at {os.path.abspath(os.path.join(base_path, view['view_name']))}")


def record_upload(interface: redshift.Interface, source: pandas.DataFrame):
    query = f'''
    insert into {interface.aws_info['records_table']}
           (  table_name,     upload_time,     rows,     redshift_user,     os_user)
    values (%(table_name)s, %(upload_time)s, %(rows)s, %(redshift_user)s, %(os_user)s)
    '''
    data = {
        'table_name': interface.full_table_name,
        'upload_time': datetime.datetime.today().strftime("%Y-%m-%d %H:%M:%S"),
        'rows': source.shape[0],
        'redshift_user': interface.aws_info['redshift_username'],
        'os_user': getpass.getuser(),  # I recognize it's not great, but hopefully no one running this is malicious. https://stackoverflow.com/a/842096/6465644
    }
    conn = interface.get_db_conn()
    cursor = conn.cursor()
    cursor.execute(query, data)
    conn.commit()


def check_coherence(upload_options: Dict, aws_info: Dict):
    upload_options = {**constants.UPLOAD_DEFAULTS, **(upload_options or {})}
    aws_info = aws_info or {}

    if upload_options['truncate_table'] is True and upload_options['drop_table'] is True:
        raise ValueError("You must only choose one. It doesn't make sense to do both")

    for c in ["redshift_username", "redshift_password", "access_key", "secret_key", "bucket", "host", "dbname", "port"]:
        if aws_info.get(c) is None:
            raise ValueError(f"You need to define {c} in the aws_info dictionary")

    return upload_options, aws_info


def upload(
    source: SourceOptions=None,
    source_args: List=None,
    source_kwargs: Dict=None,
    column_types: Dict=None,
    schema_name: str=None,
    table_name: str=None,
    upload_options: Dict=None,
    aws_info: Dict=None,
):

    source_args = source_args or []
    source_kwargs = source_kwargs or {}
    column_types = column_types or {}
    upload_options, aws_info = check_coherence(upload_options, aws_info)

    interface = redshift.Interface(schema_name, table_name, aws_info)
    source = load_source(source, source_args, source_kwargs)

    column_types = get_defined_columns(source, column_types, interface, upload_options)
    source, column_types = fix_column_types(source, column_types, interface, upload_options['drop_table'])

    if not upload_options['drop_table'] and interface.table_exists:
        compare_with_remote(source, interface)

    if upload_options['drop_table'] and interface.table_exists:
        log_dependent_views(interface)

    interface.load_to_s3(source.to_csv(None, index=False, header=False, encoding="utf-8"))
    s3_to_redshift(interface, column_types, upload_options)
    if interface.table_exists:  # still need to update those materialized views, so we can't check drop_table here
        reinstantiate_views(interface, upload_options['drop_table'], upload_options['grant_access'])
    if interface.aws_info.get("records_table") is not None:
        record_upload(interface, source)
