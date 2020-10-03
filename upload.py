import pandas
from db_interfaces import redshift


def load_source(source, source_args, source_kwargs):
    if isinstance(source, str):
        if source.endswith('.xlsx'):
            return pandas.read_excel(source, *source_args, **source_kwargs)
        if source.endswith(".csv"):
            return pandas.read_csv(source, *source_args, **source_kwargs)
    if isinstance(source, pandas.DataFrame):
        return source


def get_columns(columns, schema_name, table_name, redshift_username, redshift_password, upload_options):
    def convert_column_type_structure(columns):
        for col, typ in columns.items():
            if not isinstance(typ, dict):
                columns[col] = {"type": typ}
        return columns

    columns = convert_column_type_structure(columns)
    if upload_options['drop_table'] is False:
        existing_columns = redshift.Interface().get_columns(schema_name, table_name, redshift_username, redshift_password)
    else:
        existing_columns = {}
    return {**columns, **existing_columns}  # we prioritize existing columns, since they are generally unfixable


def upload(source=None, source_args=None, source_kwargs=None,
           columns=None, schema_name=None, table_name=None,
           redshift_username=None, redshift_password=None,
           upload_options={}):

    UPLOAD_DEFAULTS = {
        "truncate_table": False,
        "drop_table": False,
        "cleanup_s3": False,
        "grant_access": [],
    }
    upload_options = {**UPLOAD_DEFAULTS, **upload_options}

    source = load_source(source, source_args or [], source_kwargs or {})
    columns = get_columns(columns or {}, schema_name, table_name, redshift_username, redshift_password, upload_options)
    print(columns)
