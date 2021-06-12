import psycopg2
import psycopg2.sql
import boto3
import botocore
import datetime
import logging
import multiprocessing.pool
from typing import Dict, List, Tuple
if __name__ == '__main__':
    import sys, os
    sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
try:
    import base_utilities, constants
except ModuleNotFoundError:
    from .. import base_utilities, constants
log = logging.getLogger("redshift_utilities")

with base_utilities.change_directory():
    dependent_view_query = open('redshift_queries/dependent_views.sql', 'r').read()
    competing_conns_query = open('redshift_queries/competing_conns.sql', 'r').read()
    copy_table_query = open('redshift_queries/copy_table.sql', 'r').read()
    view_privileges = open('redshift_queries/view_privileges.sql').read()


class Interface:
    def __init__(self, schema_name, table_name, aws_info) -> None:
        self.name = 'redshift'
        self.aws_info = aws_info
        self.schema_name = schema_name
        self.table_name = table_name
        self.s3_name = f"{schema_name}_{table_name}_{datetime.datetime.today().strftime('%Y_%m_%d_%H_%M_%S_%f')}"

        self._db_conn = None
        self._s3_conn = None

        cursor = self.get_db_conn().cursor()
        self.full_table_name = psycopg2.sql.SQL('.').join([
            psycopg2.sql.Identifier(schema_name),
            psycopg2.sql.Identifier(table_name)
        ]).as_string(cursor)

        self.table_exists = self.check_table_exists()  # must be initialized after the _db, _s3_conn

    def get_db_conn(self) -> constants.Connection:
        """
        Gets DB connection. Caches connection for later use
        """
        if self._db_conn is None:
            self._db_conn = psycopg2.connect(
                host=self.aws_info['host'],
                dbname=self.aws_info['dbname'],
                port=self.aws_info['port'],
                user=self.aws_info['redshift_username'],
                password=self.aws_info['redshift_password'],
                connect_timeout=180,
            )
        return self._db_conn

    def get_s3_conn(self) -> constants.Connection:
        """
        Gets s3 connection to load data. Caches connection for later use.
        """
        if self._s3_conn is None:
            self._s3_conn = boto3.resource(
                "s3",
                aws_access_key_id=self.aws_info['access_key'],
                aws_secret_access_key=self.aws_info['secret_key'],
                use_ssl=False,
                region_name="us-east-1",
            )
        return self._s3_conn

    def get_columns(self) -> Dict[str, Dict[str, str]]:
        """
        Gets columns and types from PG_TABLE_DEF
        """
        def type_mapping(t) -> str:
            """
            basing off of https://www.flydata.com/blog/redshift-supported-data-types/
            """
            if "char" in t or t == "text":
                word_len = "".join(c for c in t if c.isnumeric())
                if word_len != "":
                    return f"varchar({word_len})"
                else:
                    return "varchar"
            if t == "date":
                return "date"
            if "timestamp" in t:
                return "timestamp"
            if t.startswith("bool"):
                return "boolean"
            if "int" in t:
                return "bigint"
            if t == "double precision" or "numeric" in t:
                return "double precision"
            raise ValueError(f"Unsupported type: {t}")

        query = '''
        set search_path to %(schema)s;
        select "column", type from PG_TABLE_DEF
        where tablename = %(table_name)s;
        '''
        with self.get_db_conn().cursor() as cursor:
            cursor.execute(query, {'schema': self.schema_name, 'table_name': self.table_name})
            return {col: {"type": type_mapping(t)} for col, t in cursor.fetchall()}

    def get_dependent_views(self) -> List[Dict]:
        """
        Returns a list of dictionaries containing information about views, including dependencies and source text
        """
        def get_view_query(row) -> Dict:
            view_text_query = f"set search_path = 'public';\nselect pg_get_viewdef('{row['full_name']}', true) as text"

            with self.get_db_conn().cursor() as cursor:
                cursor.execute(view_text_query)
                view_text = cursor.fetchone()[0]
            return {
                "owner": row["viewowner"],
                "dependencies": dependency_relations.get(row['full_name'], []),
                "view_name": row['full_name'],
                "text": view_text,
                "view_type": row["dependent_kind"],
            }

        def get_grants(schema_name, view_name):
            with self.get_db_conn().cursor() as cursor:
                cursor.execute(view_privileges, {'schema_name': schema_name, 'view_name': view_name})
                grants = ", ".join(x[0] for x in cursor.fetchall())
                return f"GRANT SELECT ON {schema_name}.{view_name} to {grants}"

        def format_row(row) -> Dict:
            # columns = ['dependent_schema', 'dependent_view', 'dependent_kind', 'viewowner', 'nspname', 'relname']
            return {
                'full_name': f"{row[0]}.{row[1]}",
                'dependency': f"{row[4]}.{row[5]}",
                'dependent_kind': {"m": "materialized view", "v": "view"}.get(row[2], row[2]),
                'viewowner': row[3],
                'grants': get_grants(row[0], row[1])
            }

        unsearched_views = [f"{self.schema_name}.{self.table_name}"]  # the table is searched, but will not appear in the final_df
        dependencies = []

        while len(unsearched_views):
            view = unsearched_views[0]
            with self.get_db_conn().cursor() as cursor:
                cursor.execute(dependent_view_query, {'schema_name': view.split(".", 1)[0], 'table_name': view.split(".", 1)[1]})
                data = [format_row(row) for row in cursor.fetchall()]
            dependencies.extend(data)
            unsearched_views.extend(row["full_name"] for row in data)
            unsearched_views.pop(0)

        dependency_relations = {}
        for row in dependencies:
            dependency_relations.setdefault(row['full_name'], []).append(row['dependency'])
        return [get_view_query(row) for row in dependencies]

    def load_to_s3(self, source_dfs) -> None:
        """
        Loads data to S3, using multiprocessing.pool.Threadpool to speed up process.
        """
        def loader(data) -> None:
            i, source_df = data
            s3_name = self.s3_name + str(i)
            obj = self.get_s3_conn().Object(self.aws_info['bucket'], s3_name)
            obj.delete()
            obj.wait_until_not_exists()

            try:
                response = obj.put(Body=source_df)
            except botocore.exceptions.ClientError as e:
                if "(SignatureDoesNotMatch)" in str(e):
                    raise ValueError("The error below occurred when the S3 credentials expire")
                raise BaseException

            if response["ResponseMetadata"]["HTTPStatusCode"] != 200:
                raise ValueError(f"Something unusual happened in the upload.\n{str(response)}")

            obj.wait_until_exists()

        self.get_s3_conn()  # we need an initial call to initialize the S3 conn. Otherwise the threads will simultaneously create multiple instances, causing the error here: https://stackoverflow.com/questions/52675027/why-do-i-sometimes-get-key-error-using-sqs-client
        log.info(f"Loading table to S3 in {len(source_dfs)} chunks")
        with multiprocessing.pool.ThreadPool(processes=min(len(source_dfs), constants.MAX_THREAD_COUNT)) as pool:
            pool.map(loader, enumerate(source_dfs))

    def cleanup_s3(self, parallel_loads: int) -> None:
        """
        Attempts to delete S3 files used to copy to Redshift.
        If it cannot delete, it will attempt to overwrite the S3 object for security and space savings
        """
        for i in range(parallel_loads):
            obj = self.get_s3_conn().Object(self.aws_info['bucket'], self.s3_name + str(i))
            try:
                obj.delete()
            except Exception as e:
                log.error("Could not delete {}\nException: {}".format(self.s3_name + str(i), e))
                log.error("Attempting to Overwrite with empty string to minimze storage use")
                obj.put(Body=b"")

    def get_exclusive_lock(self) -> Tuple[constants.Connection, constants.Connection]:
        """
        Uses STV_SESSIONS to find any other connections with locks on the table and then tries to kill them
        """
        conn = self.get_db_conn()
        cursor = conn.cursor()
        if not self.table_exists:  # nothing to lock against
            return conn, cursor

        log.info("Acquiring an exclusive lock on the Redshift table")
        cursor.execute(competing_conns_query, {'schema_name': self.schema_name, 'table_name': self.table_name})
        try:
            processes = set(cursor.fetchall()) - {(conn.get_backend_pid(), self.aws_info['redshift_username'])}  # we don't want to delete the connection we're on!
        except psycopg2.ProgrammingError:  # no results to fetch
            processes = set()
        for process, username in processes:
            try:
                cursor.execute(f"select pg_terminate_backend('{process}')")
            except:
                pass
        conn.commit()
        cursor.execute(f"lock table {self.full_table_name}")
        return conn, cursor

    def check_table_exists(self) -> bool:
        """
        Checks whether the table exists using pg_tables
        """
        query = '''
        select count(*) as cnt
        from pg_tables
        where schemaname = %(schema_name)s
        and tablename = %(table_name)s
        '''
        log.info("Checking if the table exists in Redshift")
        with self.get_db_conn().cursor() as cursor:
            cursor.execute(query, {'schema_name': self.schema_name, 'table_name': self.table_name})
            return cursor.fetchone()[0] != 0

    def copy_table(self, cursor) -> None:
        """
        Copies the S3 file(s) to Redshift
        """
        log.info("Copying table from S3 to Redshift")
        query = copy_table_query.format(
            file_destination=self.full_table_name,
            source=f"s3://{self.aws_info['bucket']}/{self.s3_name}",
            access=self.aws_info['access_key'],
            secret=self.aws_info['secret_key']
        )
        cursor.execute(query)

    def expand_varchar_column(self, colname, max_str_len) -> bool:
        """
        Attempts to alter a varchar column to be varchar({max_str_len})
        """
        if max_str_len > constants.MAX_VARCHAR_LENGTH:
            return False

        query = f"""
        alter table {self.full_table_name} alter column "{colname}" type varchar({max_str_len})
        """
        log.info(f"Expanding the max characters for: '{colname}'")
        conn, cursor = self.get_exclusive_lock()
        old_isolation_level = conn.isolation_level
        conn.set_isolation_level(0)
        cursor.execute(query)
        conn.commit()
        conn.set_isolation_level(old_isolation_level)
        log.info(f"'{colname}' now has a max length of: {max_str_len}")
        return True
