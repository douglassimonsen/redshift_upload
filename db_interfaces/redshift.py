import psycopg2
import pandas
import constants


class Interface:
    def get_columns(self, schema, table_name, username, password):
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
        with psycopg2.connect(
            host=constants.host,
            dbname=constants.dbname,
            port=constants.port,
            user=username,
            password=password,
            connect_timeout=180,
        ) as conn:
            columns = pandas.read_sql(query, conn, params={'schema': schema, 'table_name': table_name})
        return {col: {"type": type_mapping(t)} for col, t in zip(columns["column"], columns["type"])}
