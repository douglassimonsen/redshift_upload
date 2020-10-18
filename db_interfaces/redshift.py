import psycopg2
import pandas
import constants


dependent_view_query = open('db_interfaces/redshift_dependent_views.sql', 'r').read()


class Interface:
    def __init__(self, schema_name, table_name, redshift_username, redshift_password):
        self.name = 'redshift'
        self.schema_name = schema_name
        self.table_name = table_name
        self.redshift_username = redshift_username
        self.redshift_password = redshift_password

    def get_conn(self):
        return psycopg2.connect(
            host=constants.host,
            dbname=constants.dbname,
            port=constants.port,
            user=self.redshift_username,
            password=self.redshift_password,
            connect_timeout=180,
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
        with self.get_conn() as conn:
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

        with self.get_conn() as conn:
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
