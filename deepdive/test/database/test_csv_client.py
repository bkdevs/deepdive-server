import unittest

from pydantic_core._pydantic_core import ValidationError

from deepdive.database.csv_client import CSVClient
from deepdive.models import Database
from deepdive.schema import ColumnSchema, ColumnType, DatabaseSchema, TableSchema

TEST_DB = DatabaseSchema(
    tables=[
        TableSchema(
            name="customers",
            columns=[
                ColumnSchema(name="id", column_type=ColumnType.INT),
                ColumnSchema(name="address", column_type=ColumnType.TEXT),
            ],
        ),
    ],
    sql_dialect="Sqlite",
)


class TestCsvClient(unittest.TestCase):
    def test_schema_invalid(self):
        with self.assertRaises(ValidationError):
            CSVClient.validate(Database(schema="bad schema"))

    def test_schema_valid(self):
        CSVClient.validate(Database(schema=TEST_DB.model_dump_json(exclude_none=True)))
