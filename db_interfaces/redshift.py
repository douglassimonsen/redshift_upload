import psycopg2
import pandas
import constants
import boto3
import botocore
import datetime

dependent_view_query = open('db_interfaces/redshift_dependent_views.sql', 'r').read()
remote_cols_query = open('db_interfaces/redshift_remote_cols.sql', 'r').read()
competing_conns_query = open('db_interfaces/redshift_kill_connections.sql', 'r').read()
copy_table_query = open('db_interfaces/redshift_copy_table.sql', 'r').read()


class Interface:
    def __init__(self, schema_name, table_name, redshift_username, redshift_password, access_key, secret_key):
        self.name = 'redshift'
        self.schema_name = schema_name
        self.table_name = table_name
        self.redshift_username = redshift_username
        self.redshift_password = redshift_password
        self.access_key = access_key
        self.secret_key = secret_key

    def get_db_conn(self):
        return psycopg2.connect(
            host=constants.host,
            dbname=constants.dbname,
            port=constants.port,
            user=self.redshift_username,
            password=self.redshift_password,
            connect_timeout=180,
        )

    def get_s3_conn(self):
        return boto3.resource(
            "s3",
            aws_access_key_id=self.access_key,
            aws_secret_access_key=self.secret_key,
            use_ssl=False,
            region_name="us-east-1",
        )

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
        with self.get_db_conn() as conn:
            columns = pandas.read_sql(query, conn, params={'schema': self.schema_name, 'table_name': self.table_name})
        return {col: {"type": type_mapping(t)} for col, t in zip(columns["column"], columns["type"])}

    def get_dependent_views(self):
        def get_view_query(row, dependencies):
            view = row["dependent_schema"] + "." + row["dependent_view"]
            view_text_query = f"set search_path = 'public';\nselect pg_get_viewdef('{view}', true) as text"
            df = pandas.read_sql(view_text_query, self.db_conn)
            return {"owner": row["viewowner"], "dependencies": dependencies.get(view, []), "view_name": view, "text": df.text[0], "view_type": row["dependent_kind"]}

        unsearched_views = [f"{self.schema_name}.{self.table_name}"]  # the table is searched, but will not appear in the final_df
        final_df = pandas.DataFrame(columns=["dependent_schema", "dependent_view", "dependent_kind", "viewowner", "nspname", "relname",])

        with self.get_db_conn() as conn:
            while len(unsearched_views):
                view = unsearched_views[0]
                df = pandas.read_sql(
                    dependent_view_query,
                    conn,
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
        with self.get_db_conn() as conn:
            return pandas.read_sql(remote_cols_query, conn, params={'table_name': self.table_name})['attname'].to_list()

    def load_to_s3(self, source_df):
        print(source_df)
        self.s3_name = f"{self.schema_name}_{self.table_name}_{datetime.datetime.today().strftime('%Y_%m_%d_%H_%M_%S_%f')}"
        s3_conn = self.get_s3_conn()
        obj = s3_conn.Object(constants.bucket, self.s3_name)
        obj.delete()
        obj.wait_until_not_exists()

        try:
            response = obj.put(Body=source_df)
        except botocore.exceptions.ClientError as e:
            if "(SignatureDoesNotMatch)" in str(e):
                raise ValueError("The error below occurred when the S3 credentials expire. As Data Analytics to distribute new ones")
            raise BaseException

        if response["ResponseMetadata"]["HTTPStatusCode"] != 200:
            raise ValueError(f"Something unusual happened in the upload.\n{str(response)}")

        obj.wait_until_exists()

    def get_exclusive_lock(self):
        with self.get_db_conn() as conn:
            processes = pandas.read_sql(competing_conns_query, conn, params={'table_name': self.table_name})
            processes = processes[processes["pid"] != conn.get_backend_pid()]
            for _, row in processes.iterrows():
                try:
                    conn.cursor().execute(f"select pg_terminate_backend('{row['pid']}')")
                except Exception as exc:
                    pass
            conn.commit()

    def copy_table(self):
        query = copy_table_query.format(
            file_destination=f"{self.schema_name}.{self.table_name}",
            source=f"s3://{constants.bucket}/{self.s3_name}",
            access=self.access_key,
            secret=self.secret_key
        )
        with self.get_db_conn() as conn:
            cursor = conn.cursor()
            cursor.execute(query)
            conn.commit()
