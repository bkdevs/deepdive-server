import itertools
import logging
from typing import List

from google.cloud.bigquery.schema import SchemaField

from deepdive.database.bigquery_client import _get_bigquery_client
from deepdive.database.schema_client import SchemaClient
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


# Taken from: https://cloud.google.com/bigquery/docs/reference/rest/v2/tables#TableFieldSchema.FIELDS.type
def _parse_bigquery_type(bigquery_type: str) -> str:
    if bigquery_type == "STRING":
        return ColumnType.TEXT
    elif bigquery_type == "INTEGER" or bigquery_type == "INT64":
        return ColumnType.INT
    elif bigquery_type == "FLOAT" or bigquery_type == "FLOAT64":
        return ColumnType.FLOAT
    elif bigquery_type == "BOOLEAN" or bigquery_type == "BOOL":
        return ColumnType.BOOLEAN
    elif bigquery_type == "TIMESTAMP" or bigquery_type == "TIME":
        return ColumnType.TIME
    elif bigquery_type == "DATE" or bigquery_type == "DATETIME":
        return ColumnType.DATE
    elif bigquery_type == "RECORD":
        return ColumnType.RECORD

    logger.error(
        f"Could not translate BigQuery data_type: {bigquery_type}, defaulting to TEXT"
    )
    return ColumnType.TEXT


class BigQuerySchemaClient(SchemaClient):
    def initialize(self, database: Database):
        self.database = database
        self.client = _get_bigquery_client()

    def fetch(self) -> DatabaseSchema:
        return DatabaseSchema(
            sql_dialect=SqlDialect.GOOGLE_SQL, tables=self._fetch_tables()
        )

    def _fetch_tables(self) -> List[TableSchema]:
        tables = []
        for table in self.client.list_tables(self.database.bigquery_dataset_id):
            table = self.client.get_table(
                f"{self.database.bigquery_dataset_id}.{table.table_id}"
            )
            columns = list(
                itertools.chain(*[self._get_columns(field) for field in table.schema])
            )
            tables.append(TableSchema(name=table.table_id, columns=columns))
        return tables

    def _get_columns(self, field: SchemaField) -> List[ColumnSchema]:
        if field.mode == "REPEATED":
            logger.error("BigQuery dataset has REPEATED field, skipping: " + str(field))
            return []

        if field.field_type == "RECORD":  # denormalize recursively
            subcolumns = list(
                itertools.chain(
                    *[self._get_columns(subfield) for subfield in field.fields]
                )
            )
            return [
                ColumnSchema(
                    name=f"{field.name}.{column.name}",
                    column_type=column.column_type,
                    comment=column.comment,
                )
                for column in subcolumns
            ]

        return [
            ColumnSchema(
                name=field.name,
                column_type=_parse_bigquery_type(field.field_type),
                comment=str(field.description),
            )
        ]

    def _fetch_foreign_keys(self) -> List[ForeignKey]:
        return None

    def _fetch_primary_keys(self) -> List[str]:
        """
        possible to get constraints using TABLE_CONSTRAINTS
        e.g, client.query("select * from bigquery-public-data.austin_bikeshare.INFORMATION_SCHEMA.TABLE_CONSTRAINTS")

        but TABLE_CONSTRAINTS are not so common nor enforced: https://cloud.google.com/bigquery/docs/information-schema-table-constraints
        """
        return []
