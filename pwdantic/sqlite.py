import sqlite3
from typing import Any

from pwdantic.interfaces import PWEngine
from pwdantic.serialization import SQLColumn

sqlite_column = tuple[int, str, str, int, Any, int]


class SQLiteEngineError(Exception):
    pass


class SqliteEngine(PWEngine):
    def __init__(self, conn: sqlite3.Connection):
        self.conn = conn
        self.cursor = conn.cursor()

    def __del__(self):
        self.conn.close()

    def _represent_bytes(self, data: bytes) -> str:
        return f"X'{data.hex().upper()}'"

    def select(
        self, field: str, table: str, conditions: dict[str, Any] | None = None
    ) -> list[Any]:

        if conditions is None:
            query = f"SELECT {field} FROM {table}"
            self.cursor.execute(query)

        else:
            where_clause = " AND ".join(
                f"{key} = ?" for key in conditions.keys()
            )
            query = f"SELECT {field} FROM {table} WHERE {where_clause}"
            self.cursor.execute(query, tuple(conditions.values()))

        return self.cursor.fetchall()

    def insert(self, table: str, obj_data: dict[str, Any]) -> int:
        cols = [col for col, val in obj_data.items() if val != None]
        vals = [val for val in obj_data.values() if val != None]

        col_str = ", ".join(cols)
        val_str = ", ".join(["?"] * len(vals))

        query = f"INSERT INTO {table} ({col_str}) VALUES({val_str})"

        self.cursor.execute(query, tuple(vals))
        self.conn.commit()
        return self.cursor.lastrowid

    def _transfer_type_from_standard(self, str_type: str) -> str:
        types = {
            "integer": "INTEGER",
            "date-time": "TIMESTAMP",
            "string": "TEXT",
            "number": "REAL",
            "boolean": "BOOLEAN",
            "bytes": "BLOB",
        }

        return types[str_type]

    def _transfer_type_to_standard(self, str_type: str) -> str:
        types = {
            "INTEGER": "integer",
            "TIMESTAMP": "date-time",
            "TEXT": "string",
            "REAL": "number",
            "BOOLEAN": "boolean",
            "BLOB": "bytes",
        }

        return types[str_type]

    def _create_new(self, classname: str, standard_cols: list[SQLColumn]):
        sqlite_cols = []
        for column in standard_cols:
            lite_col = f"{column.name} {self._transfer_type_from_standard(column.datatype)}"

            if column.nullable and not column.primary_key:
                lite_col += " NULLABLE"
            else:
                lite_col += " NOT NULL"

            if column.primary_key:
                lite_col += " PRIMARY KEY AUTOINCREMENT"

            if column.unique:
                lite_col += " UNIQUE"

            if column.default is not None:
                if column.datatype != "bytes":
                    lite_col += f" DEFAULT {column.default}"
                else:
                    lite_col += (
                        f" DEFAULT {self._represent_bytes(column.default)}"
                    )

            sqlite_cols.append(lite_col)

        self.cursor.execute(
            f"CREATE TABLE IF NOT EXISTS {classname} ({','.join(sqlite_cols)})"
        )

        self.conn.commit()

    def _parse_raw_column(self, column: str) -> SQLColumn:
        column_data: list[str] = column.split(" ")
        column_name = column_data.pop(0)
        column_type = self._transfer_type_to_standard(column_data.pop(0))

        nullable = None
        primary = None
        default = None
        unique = False

        while len(column_data) > 0:
            current = column_data.pop(0)
            match current:
                case "NOT":
                    column_data.pop(0)
                    nullable = False
                case "NULLABLE":
                    nullable = True
                case "DEFAULT":
                    default = column_data.pop(0)
                case "PRIMARY" | "KEY" | "AUTOINCREMENT":
                    primary = True
                case "UNIQUE":
                    unique = True
                case _:
                    raise SQLiteEngineError(
                        "Failed parsing current DB schema"
                    )

        return SQLColumn(
            column_name, column_type, nullable, default, primary, unique
        )

    def _get_SQLColumns(self, table: str) -> list[SQLColumn]:
        query = f"SELECT sql FROM sqlite_master WHERE type='table' AND name='{table}';"
        self.cursor.execute(query)
        current_schema = self.cursor.fetchone()[0]
        clean_schema: str = current_schema.split("(")[1].split(")")[0]
        raw_cols = clean_schema.split(",")

        standard_cols = []
        for raw_col in raw_cols:
            standard_cols.append(self._parse_raw_column(raw_col))

        return standard_cols

    def _migrate_from(self, table: str, new_columns: list[SQLColumn]):
        for col in new_columns:
            if col.datatype == "bytes" and col.default is not None:
                col.default = self._represent_bytes(col.default)
                
            print(col)

        print()

        current_columns = self._get_SQLColumns(table)
        for col in current_columns:
            print(col)

    def migrate(self, table: str, columns: list[SQLColumn]):
        matched_tables = self.cursor.execute(
            f"SELECT name FROM sqlite_master WHERE type='table' AND name='{table}';"
        ).fetchall()

        if len(matched_tables) == 0:
            return self._create_new(table, columns)

        else:
            return self._migrate_from(table, columns)

    def update(self, table: str, obj_data: dict[str, Any], primary_key: str):
        cols = [col for col, val in obj_data.items() if val != None]
        vals = [val for val in obj_data.values() if val != None]

        set_string = ""
        for col in cols:
            set_string += f"{col} = ?, "
        set_string = set_string[:-2]

        query = f"UPDATE {table} SET {set_string} WHERE {primary_key} = ?"
        vals.append(obj_data[primary_key])

        self.cursor.execute(query, tuple(vals))
        self.conn.commit()

    def delete(self, table: str, key: str, value: Any):
        query = f"DELETE FROM {table} WHERE {key} = ?"
        self.cursor.execute(query, (value,))
        self.conn.commit()
