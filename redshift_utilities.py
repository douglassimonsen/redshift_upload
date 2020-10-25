import json
import base_utilities
import os
import pandas
import toposort
import datetime
import psycopg2
import getpass
from typing import Dict, List
from db_interfaces import redshift


def log_dependent_views(interface: redshift.Interface):
    def log_query(metadata: Dict):
        metadata["text"] = f"set search_path = '{interface.schema_name}';\nCREATE {metadata.get('view_type', 'view')} {metadata['view_name']} as\n{metadata['text']}"
        base_path = f"temp_view_folder/{interface.name}/{interface.table_name}"
        base_file = f"{base_path}/{metadata['view_name']}"
        os.makedirs(base_path, exist_ok=True)

        with open(f"{base_file}.txt", "w") as f:
            json.dump(metadata, f)

    dependent_views = interface.get_dependent_views()
    with base_utilities.change_directory():
        for view_metadata in dependent_views:
            log_query(view_metadata)


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
    with base_utilities.change_directory():
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
