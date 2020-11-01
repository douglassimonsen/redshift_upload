import psycopg2
import psycopg2.sql
import pandas
import boto3
import botocore
import datetime
import logging
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
    remote_cols_query = open('redshift_queries/remote_cols.sql', 'r').read()
    competing_conns_query = open('redshift_queries/kill_connections.sql', 'r').read()
    copy_table_query = open('redshift_queries/copy_table.sql', 'r').read()


class Interface:
    def __init__(self, schema_name, table_name, aws_info):
        self.name = 'redshift'
        self.aws_info = aws_info
        self.schema_name = schema_name
        self.table_name = table_name

        self._db_conn = None
        self._s3_conn = None

        cursor = self.get_db_conn().cursor()
        self.full_table_name = psycopg2.sql.SQL('.').join([
            psycopg2.sql.Identifier(schema_name),
            psycopg2.sql.Identifier(table_name)
        ]).as_string(cursor)

        self.table_exists = self.check_table_exists()  # must be initialized after the _db, _s3_conn

    def get_db_conn(self):
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

    def get_s3_conn(self):
        if self._s3_conn is None:
            self._s3_conn = boto3.resource(
                "s3",
                aws_access_key_id=self.aws_info['access_key'],
                aws_secret_access_key=self.aws_info['secret_key'],
                use_ssl=False,
                region_name="us-east-1",
            )
        return self._s3_conn

    def get_columns(self):
        def type_mapping(t):
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
            if t == "boolean":
                return "boolean"
            if "int" in t:
                return "bigint"
            if t == "double precision" or "numeric" in t:
                return "double precision"
            return t

        query = '''
        set search_path to %(schema)s;
        select "column", type from PG_TABLE_DEF
        where tablename = %(table_name)s;
        '''
        columns = pandas.read_sql(query, self.get_db_conn(), params={'schema': self.schema_name, 'table_name': self.table_name})
        return {col: {"type": type_mapping(t)} for col, t in zip(columns["column"], columns["type"])}

    def get_dependent_views(self):
        def get_view_query(row, dependencies):
            view = row["dependent_schema"] + "." + row["dependent_view"]
            view_text_query = f"set search_path = 'public';\nselect pg_get_viewdef('{view}', true) as text"

            df = pandas.read_sql(view_text_query, self.get_db_conn())
            return {"owner": row["viewowner"], "dependencies": dependencies.get(view, []), "view_name": view, "text": df.text[0], "view_type": row["dependent_kind"]}

        unsearched_views = [self.full_table_name]  # the table is searched, but will not appear in the final_df
        final_df = pandas.DataFrame(columns=["dependent_schema", "dependent_view", "dependent_kind", "viewowner", "nspname", "relname",])

        while len(unsearched_views):
            view = unsearched_views[0]
            df = pandas.read_sql(
                dependent_view_query,
                self.get_db_conn(),
                params={
                    'schema_name': view.split(".", 1)[0],
                    'table_name': view.split(".", 1)[1]
                }
            )
            final_df = final_df.append(df, ignore_index=True)
            unsearched_views.extend([f'{row["dependent_schema"]}.{row["dependent_view"]}' for i, row in df.iterrows()])
            unsearched_views.pop(0)

        try:
            final_df["name"] = final_df.apply(lambda row: f'{row["dependent_schema"]}.{row["dependent_view"]}', axis=1)
            final_df["discrepancy"] = final_df.apply(lambda row: f'{row["nspname"]}.{row["relname"].lstrip("_")}', axis=1)
            final_df["dependent_kind"] = final_df["dependent_kind"].replace({"m": "materialized view", "v": "view"})

            dependencies = final_df[["name", "discrepancy"]].groupby("name").apply(lambda tmp_df: tmp_df["discrepancy"].drop_duplicates().tolist()).reset_index()
            dependencies.columns = ["name", "dependencies"]
            dependencies = dict(zip(dependencies["name"], dependencies["dependencies"]))
        except ValueError:
            dependencies = {}
        return [get_view_query(row, dependencies) for i, row in final_df.iterrows()]

    def get_remote_cols(self):
        return pandas.read_sql(remote_cols_query, self.get_db_conn(), params={'table_name': self.table_name})['attname'].to_list()

    def load_to_s3(self, source_dfs):
        self.s3_name = f"{self.schema_name}_{self.table_name}_{datetime.datetime.today().strftime('%Y_%m_%d_%H_%M_%S_%f')}"

        for i, source_df in enumerate(source_dfs):
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

    def cleanup_s3(self, parallel_loads: int):
        for i in range(parallel_loads):
            obj = self.s3_conn.Object(self.aws_info['bucket'], self.s3_name + str(i))
            try:
                obj.delete()
            except Exception as e:
                log.error("Could not delete {}\nException: {}".format(self.s3_name + str(i), e))
                log.error("Attempting to Overwrite with empty string to minimze storage use")
                obj.put(Body=b"")

    def get_exclusive_lock(self):
        conn = self.get_db_conn()
        cursor = conn.cursor()
        if not self.table_exists:  # nothing to lock against,
            return conn, cursor

        processes = pandas.read_sql(competing_conns_query, conn, params={'table_name': self.table_name})
        processes = processes[processes["pid"] != conn.get_backend_pid()]
        for _, row in processes.iterrows():
            try:
                cursor.execute(f"select pg_terminate_backend('{row['pid']}')")
            except Exception as exc:
                pass
        conn.commit()
        cursor.execute(f"lock table {self.full_table_name}")
        return conn, cursor

    def check_table_exists(self):
        query = '''
        select count(*) as cnt
        from pg_tables
        where schemaname = %(schema_name)s
        and tablename = %(table_name)s
        '''
        table_count = pandas.read_sql(
            query,
            self.get_db_conn(),
            params={
                'schema_name': self.schema_name,
                'table_name': self.table_name
            }
        )['cnt'].iat[0]
        return table_count != 0

    def copy_table(self, cursor):
        query = copy_table_query.format(
            file_destination=self.full_table_name,
            source=f"s3://{self.aws_info['bucket']}/{self.s3_name}",
            access=self.aws_info['access_key'],
            secret=self.aws_info['secret_key']
        )
        cursor.execute(query)

    def expand_varchar_column(self, colname, max_str_len):
        if max_str_len > constants.MAX_VARCHAR_LENGTH:
            return False

        query = f"""
        alter table {self.full_table_name} alter column "{colname}" type varchar({max_str_len})
        """
        conn, cursor = self.get_exclusive_lock()
        old_isolation_level = conn.isolation_level
        conn.set_isolation_level(0)
        cursor.execute(query)
        conn.commit()
        conn.set_isolation_level(old_isolation_level)
        print(f"'{colname}' should have length {max_str_len}")
        return True
