from airflow.hooks.base import BaseHook
from typing import Literal
import psycopg2

class CustomPostgresHook(BaseHook):
    def __init__(self, postgres_conn_id: str, **kwargs):
        self.postgres_conn_id = postgres_conn_id
        self.conn = None
        self.database = kwargs.get("database")

    def get_conn(self) -> psycopg2.extensions.connection:
        conn = BaseHook.get_connection(self.postgres_conn_id)
        conn_args = {
            "host": conn.host,
            "user": conn.login,
            "password": conn.password,
            "dbname": self.database or conn.schema,
            "port": conn.port,
        }
        self.conn = psycopg2.connect(**conn_args)
        return self.conn

    def bulk_load(self, table: str, filename: str, encoding="utf-8",
                if_exists: Literal["append","replace"]="append", sep=',', with_header=True):
        create = self._create_table_sql(table, filename, encoding, sep, with_header)
        replace = "TRUNCATE TABLE {};".format(table) if if_exists == "replace" else str()
        copy = "COPY {} FROM STDIN DELIMITER '{}' {};".format(table, sep, ("CSV HEADER" if with_header else "CSV"))
        sql = ''.join([create, replace, copy])
        self.copy_expert(sql, filename, encoding)

    def _create_table_sql(self, table: str, filename: str, encoding="utf-8", sep=',', with_header=True) -> str:
        if with_header:
            column_list = self._read_csv_column_list(filename, encoding, sep)
            return "CREATE TABLE IF NOT EXISTS {}({});".format(table, column_list)
        else:
            return str()

    def _read_csv_column_list(self, filename: str, encoding="utf-8", sep=',') -> str:
        import csv
        def is_int4_type(value: str) -> bool:
            return (not value) or (value.isdigit() and (-2147483648 <= int(value) <= 2147483647))
        with open(filename, "r+", encoding=encoding) as file:
            reader = csv.reader(file, delimiter=sep)
            header = next(reader)
            dtypes = [all(map(is_int4_type, values)) for values in zip(*[next(reader) for _ in range(5)])]
            return ", ".join(["{} {}".format(col, ("int4" if is_int4 else "text")) for col, is_int4 in zip(header, dtypes)])

    def copy_expert(self, sql: str, filename: str, encoding="utf-8") -> None:
        from contextlib import closing
        self.log.info("Running copy expert: %s, filename: %s", sql, filename)
        with open(filename, "r+", encoding=encoding) as file, closing(self.get_conn()) as conn, closing(conn.cursor()) as cur:
            cur.copy_expert(sql, file)
            file.truncate(file.tell())
            conn.commit()