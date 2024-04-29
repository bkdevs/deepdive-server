import os
import sqlite3
import uuid
import math
from abc import abstractmethod
from typing import Dict

import pandas as pd

from deepdive.database.client import DatabaseClient
from deepdive.database.file_based_client_helper import validate_column_name
from deepdive.models import Database, DatabaseFile
from deepdive.schema import ColumnType, DatabaseSchema, TableSchema


class FileBasedClient(DatabaseClient):
    """
    The base DatabaseClient class to handle file based DBs.
    """

    # sqlite3 by default supports null, int, real, text and blob
    SUPPORTED_COLUMN_TYPES = [
        ColumnType.INT,
        ColumnType.FLOAT,
        ColumnType.TEXT,
    ]

    BASE_DIRECTORY = "local_dbs"
    DB_NAME = "temp.db"

    def initialize(self, database: Database):
        self.db_schema = DatabaseSchema.model_validate_json(database.schema)
        self.db_path = self._setup_directories()
        self.conn = sqlite3.connect(self.db_path)
        self._define_sqlite_functions(self.conn)
        table_schemas = {table.name: table for table in self.db_schema.tables}
        for db_file in database.files.all():
            self._parse_file(db_file, table_schemas)
        self.conn.commit()

    def _define_sqlite_functions(self, conn):
        conn.create_function("log10", 1, math.log10)

    def finalize(self):
        self.conn.close()
        if os.path.exists(self.db_path):
            os.remove(self.db_path)
        temp_dir_path = os.path.dirname(self.db_path)
        if os.path.exists(temp_dir_path):
            os.rmdir(temp_dir_path)

    def validate(database: Database):
        schema = DatabaseSchema.model_validate_json(database.schema, strict=True)
        for table in schema.tables:
            for column in table.columns:
                validate_column_name(column.name)

    def execute_query(self, query: str) -> pd.DataFrame:
        return pd.read_sql_query(query, self.conn)

    @abstractmethod
    def read_data(self, db_file: DatabaseFile) -> Dict[str, pd.DataFrame]:
        """
        A method to read and parse the given file and return a dictionary
        that maps table name to the corresponding pandas DataFrame.
        """
        return {}

    def _setup_directories(self) -> str:
        temp_dir_path = f"{FileBasedClient.BASE_DIRECTORY}/{uuid.uuid4()}"
        os.mkdir(temp_dir_path)
        db_path = f"{temp_dir_path}/{FileBasedClient.DB_NAME}"
        return os.path.abspath(db_path)

    def _parse_file(self, db_file: DatabaseFile, table_schemas: Dict):
        data = self.read_data(db_file)
        for table_name, dataframe in data.items():
            table_schema = table_schemas[table_name]
            self._create_table(table_schema)
            self._process_data(table_schema, dataframe)
            self._insert_data(table_schema, dataframe)

    def _create_table(self, schema: TableSchema):
        column_descriptions = []
        for column in schema.columns:
            column_descriptions.append(
                f"{column.name} {self._get_sqlite_type(column.column_type)}"
            )
        query = f"CREATE TABLE IF NOT EXISTS {schema.name} ({','.join(column_descriptions)});"
        self.conn.cursor().execute(query)

    def _get_sqlite_type(self, column_type: ColumnType) -> str:
        if column_type == ColumnType.INT:
            return "integer"
        elif column_type == ColumnType.FLOAT:
            return "real"
        return "text"

    def _insert_data(self, schema: TableSchema, data: pd.DataFrame):
        column_names = [column.name for column in schema.columns]
        query = f"INSERT INTO {schema.name}({','.join(column_names)}) VALUES({','.join(['?'] * len(column_names))})"
        data.apply(lambda row: self.conn.cursor().execute(query, row), axis=1)

    def _process_data(self, schema: TableSchema, data: pd.DataFrame):
        column_types = {column.name: column.column_type for column in schema.columns}
        for column_name in data:
            column_type = column_types[column_name]
            if column_type not in FileBasedClient.SUPPORTED_COLUMN_TYPES:
                data[column_name] = data[column_name].astype(str)
