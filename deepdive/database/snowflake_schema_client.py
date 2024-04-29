import json
import logging
from typing import List

from pandas import DataFrame
from snowflake.snowpark import Session

from deepdive.database.schema_client import SchemaClient
from deepdive.database.snowflake_helper import infer_missing_dtypes
from deepdive.models import Database
from deepdive.schema import (
    ColumnSchema,
    ColumnType,
    DatabaseSchema,
    ForeignKey,
    SqlDialect,
    TableSchema,
)

logger = logging.getLogger(__name__)


# Taken from: https://docs.snowflake.com/en/sql-reference/sql/show-columns
def _parse_snowflake_type(data_type: str) -> ColumnType:
    data_json = json.loads(data_type)
    data_type = data_json["type"]

    if data_type == "FIXED":
        return ColumnType.INT if data_json["scale"] == 0 else ColumnType.FLOAT
    if data_type == "REAL":
        return ColumnType.FLOAT
    elif data_type == "TEXT":
        return ColumnType.TEXT
    elif data_type == "BOOLEAN":
        return ColumnType.BOOLEAN
    elif data_type == "DATE":
        return ColumnType.DATE
    elif data_type == "TIME":
        return ColumnType.TIME

    logger.error(
        f"Could not translate Snowflake data_type: {data_type}, defaulting to TEXT"
    )
    return ColumnType.TEXT


class SnowflakeSchemaClient(SchemaClient):
    def initialize(self, database: Database):
        self.database = database
        self.session = Session.builder.configs(
            {
                "user": database.username,
                "password": database.password,
                "account": database.snowflake_account,
                "database": database.snowflake_database,
                "schema": database.snowflake_schema,
            }
        ).create()

    def fetch(self) -> DatabaseSchema:
        return DatabaseSchema(
            tables=self._fetch_tables(),
            primary_keys=self._fetch_primary_keys(),
            foreign_keys=self._fetch_foreign_keys(),
            sql_dialect=SqlDialect.SNOWFLAKE_SQL,
        )

    def _fetch_tables(self) -> List[TableSchema]:
        tables = []
        table_df = self.session.sql("show tables")
        table_names = [row["name"] for row in table_df.collect()]
        for table_name in table_names:
            rows = self.session.sql(f"show columns in table {table_name}").collect()
            columns = [
                ColumnSchema(
                    name=row["column_name"],
                    column_type=_parse_snowflake_type(row["data_type"]),
                )
                for row in rows
            ]
            tables.append(TableSchema(name=table_name, columns=columns))

        return tables

    def _fetch_foreign_keys(self) -> List[ForeignKey]:
        return None

    def _fetch_primary_keys(self) -> List[str]:
        rows = self.session.sql(
            f"show primary keys in schema {self.database.snowflake_database}.{self.database.snowflake_schema}"
        ).collect()
        primary_keys = [row["table_name"] + "." + row["column_name"] for row in rows]
        return primary_keys

    def execute_query(self, query: str) -> DataFrame:
        snowpark_df = self.session.sql(query)
        pandas_df = snowpark_df.to_pandas()
        return infer_missing_dtypes(snowpark_df, pandas_df)
