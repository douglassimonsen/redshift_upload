import json
import os
import toposort  # type: ignore
import datetime
import psycopg2  # type: ignore
import psycopg2.sql  # type: ignore
from typing import Dict, List, Union

try:
    import base_utilities, local_utilities  # type: ignore
    from db_interfaces import redshift  # type: ignore
except ModuleNotFoundError:
    from . import base_utilities, local_utilities
    from .db_interfaces import redshift  # type: ignore
import logging

log = logging.getLogger("redshift_utilities")


def log_dependent_views(interface: redshift.Interface) -> None:
    """
    Gets dependent views and saves them locally for reinstantiation after table is regenerated.
    """

    def log_query(metadata: Dict) -> None:
        metadata[
            "text"
        ] = f"set search_path = '{interface.schema_name}';\nCREATE {metadata.get('view_type', 'view')} {metadata['view_name']} as\n{metadata['text']}"
        base_path = f"temp_view_folder/{interface.name}/{interface.table_name}"
        base_file = f"{base_path}/{metadata['view_name']}"
        os.makedirs(base_path, exist_ok=True)

        with open(f"{base_file}.txt", "w") as f:
            json.dump(metadata, f)

    log.info("Logging any dependent views")
    dependent_views = interface.get_dependent_views()
    with base_utilities.change_directory():
        for view_metadata in dependent_views:
            log_query(view_metadata)


def get_defined_columns(
    columns: Dict, interface: redshift.Interface, upload_options: Dict
) -> Dict[str, Dict[str, str]]:
    """
    Formats the simple column type format {column: type} to {column: {"type": type}}
    Also gets column information from existing db table, if one is not dropping the table
    """

    def parse_to_dict(col: Union[str, Dict]) -> Dict:
        if isinstance(col, dict):
            return col

        if not col.startswith("VARCHAR"):
            return {"type": col, "suffix": None}

        return {"type": "VARCHAR", "suffix": int(col[8:-1])}

    columns = {col: parse_to_dict(typ) for col, typ in columns.items()}
    if upload_options["drop_table"] is False:
        existing_columns = {
            k: parse_to_dict(v) for k, v in interface.get_columns().items()
        }
    else:
        existing_columns = {}
    return {
        **columns,
        **existing_columns,
    }  # we prioritize existing columns, since they are generally unfixable


def compare_with_remote(
    source: local_utilities.Source, upload_options: Dict, interface: redshift.Interface
) -> None:
    """
    Checks to see if there are any columns in the local table that's not in the database table or vice versa.
    If the column exists in the database and not the local table, it fills that column with Nones.
    If the column exists in the local and not the database table, it raises an error and generate the SQL to manually add the columns to the table
    """
    log.info("Getting column types from the existing Redshift table")
    remote_cols = list(interface.get_columns().keys())
    remote_cols_set = set(remote_cols)

    local_cols = set(source.fieldnames)

    if not local_cols.issubset(
        remote_cols_set
    ):  # means there are new columns in the local data
        alter_queries = [
            f"""Alter table {interface.full_table_name} 
            add column {col} {source.column_types[col]['type']} 
            default null;"""
            for col in local_cols.difference(remote_cols_set)
        ]
        if not upload_options["allow_alter_table"]:
            log.error(
                "If these new columns are not a mistake, you may add them to the table by running:\n"
                + "\n".join(alter_queries)
            )
            raise NotImplementedError(
                "Haven't implemented adding new columns to the remote table yet"
            )
        else:
            conn = interface.get_db_conn()
            with conn.cursor() as cursor:
                for query in alter_queries:
                    cursor.execute(query)
            conn.commit()


def s3_to_redshift(
    interface: redshift.Interface,
    column_types: Dict,
    upload_options: Dict,
    source: local_utilities.Source,
) -> None:
    """
    Copies the data from S3 to Redshift. Also drops, creates, truncates, and grants access (if applicable)
    """

    def delete_table() -> None:
        log.info("Dropping Redshift table")
        cursor.execute(f"drop table if exists {interface.full_table_name} cascade")

    def truncate_table() -> None:
        log.info("Truncating Redshift table")
        cursor.execute(f"truncate {interface.full_table_name}")

    def create_table() -> None:
        def get_col(col_name, col_type):
            base = (
                psycopg2.sql.SQL("")
                .join(
                    [
                        psycopg2.sql.Identifier(col_name),
                    ]
                )
                .as_string(cursor)
            )  # for some reason, this is the only way to get 'a b' -> '"a b"'
            base += f" {col_type['type']}"
            if col_type["type"] == "VARCHAR" and col_type["suffix"] is not None:
                base += f"({col_type['suffix']})"
            for opt in ["distkey", "sortkey"]:
                if upload_options[opt] == col_name:
                    base += f" {opt}"
            return base

        columns = ", ".join(
            get_col(col_name, col_type) for col_name, col_type in column_types.items()
        )
        log.info("Creating Redshift table")
        cursor.execute(
            f'create table if not exists {interface.full_table_name} ({columns}) diststyle {upload_options["diststyle"]}'
        )

    def grant_access() -> None:
        grant = f"GRANT SELECT ON {interface.full_table_name} TO {', '.join(upload_options['grant_access'])}"
        log.info("Granting permissions on Redshift table")
        cursor.execute(grant)

    conn, cursor = interface.get_exclusive_lock()

    if upload_options["drop_table"] and interface.table_exists:
        delete_table()
    if upload_options["drop_table"] or not interface.table_exists:
        create_table()
    elif upload_options[
        "truncate_table"
    ]:  # we're not going to truncate if the table doesn't exist yet
        truncate_table()

    formatted_cols = [
        psycopg2.sql.SQL("")
        .join(
            [
                psycopg2.sql.Identifier(col_name),
            ]
        )
        .as_string(cursor)
        for col_name in column_types.keys()
    ]
    interface.copy_table(cursor, formatted_cols)

    # we can't ensure the grant permissions have changed, so we always do it in case
    if upload_options["grant_access"]:
        grant_access()

    conn.commit()


def reinstantiate_views(
    interface: redshift.Interface, drop_table: bool, grant_access: List
) -> None:
    """
    Using the dependency metadata saved about each view, creates a topological ordering of the views and creates each one.
    Grants the same access to the views as the table
    """

    def gen_order(views: Dict):
        base_table = {f"{interface.schema_name}.{interface.table_name}"}
        dependencies = {}
        for view in views.values():
            dependencies[view["view_name"]] = set(view["dependencies"]) - base_table
        return toposort.toposort_flatten(dependencies)

    age_limit = datetime.datetime.today() - datetime.timedelta(hours=4)
    views = {}
    base_path = f"temp_view_folder/{interface.name}/{interface.table_name}"
    log.info("Collecting Redshift views to reinstantiate")
    with base_utilities.change_directory():
        if not os.path.exists(base_path):  # no views to reinstate
            return
        possible_views = [
            os.path.join(base_path, view)
            for view in os.listdir(base_path)
            if view.endswith(".txt")
        ]  # stupid thumbs.db ruining my life
        for f in possible_views:
            if datetime.datetime.fromtimestamp(os.path.getmtime(f)) > age_limit:
                with open(f, "r") as fl:
                    view_info = json.load(fl)
                views[view_info["view_name"]] = view_info

    reload_order = gen_order(views)

    log.info("Reinstantiating Redshift views")
    with base_utilities.change_directory():
        for view_name in reload_order:
            view = views[view_name]
            conn = interface.get_db_conn(user=view["owner"])
            cursor = conn.cursor()
            try:
                if drop_table is True:
                    cursor.execute(view["text"])
                    if grant_access:
                        cursor.execute(view["grants"])
                    log.info(f"Reinstantiated: {view['view_name']}")
                elif (
                    view.get("view_type", "view") == "view"
                ):  # if there isn't a drop_table, the views still exist and we don't need to do anything
                    pass
                else:  # only get here when complete_refresh is False and view_type is materialized view
                    cursor.execute(f"refresh materialized view {view['view_name']}")
                    cursor.close()
                conn.commit()
                os.remove(f'{base_path}/{view["view_name"]}' + ".txt")
            except psycopg2.ProgrammingError:  # if the type of column changed, a view can disapper.
                conn.rollback()
                log.warning(f"We were unable to load view: {view_name}")
                log.warning(
                    f"You can see the view body at {os.path.abspath(os.path.join(base_path, view['view_name']))}"
                )
