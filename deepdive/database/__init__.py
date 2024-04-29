from typing import Dict, List

from django.core.files.uploadedfile import UploadedFile

from deepdive.database.bigquery_client import BigQueryClient
from deepdive.database.bigquery_schema_client import BigQuerySchemaClient
from deepdive.database.client import DatabaseClient
from deepdive.database.csv_client import CSVClient
from deepdive.database.excel_client import ExcelClient
from deepdive.database.file_based_client import FileBasedClient
from deepdive.database.parquet_client import ParquetClient
from deepdive.database.snowflake_client import SnowflakeClient
from deepdive.database.snowflake_schema_client import SnowflakeSchemaClient
from deepdive.models import Database, DatabaseType
from deepdive.schema import DatabaseSchema, TableConfig, TablePreview


def get_db_client(database: Database) -> DatabaseClient:
    if database.database_type == DatabaseType.SNOWFLAKE:
        return SnowflakeClient(database)
    elif database.database_type == DatabaseType.BIGQUERY:
        return BigQueryClient(database)
    elif database.database_type == DatabaseType.CSV:
        return CSVClient(database)
    elif database.database_type == DatabaseType.EXCEL:
        return ExcelClient(database)
    elif database.database_type == DatabaseType.PARQUET:
        return ParquetClient(database)
    else:
        raise Exception("Unsupported database type! " + database.database_type)


def validate_db(database: Database):
    if database.database_type == DatabaseType.SNOWFLAKE:
        SnowflakeClient.validate(database)
    elif database.database_type == DatabaseType.BIGQUERY:
        BigQueryClient.validate(database)
    elif database.database_type == DatabaseType.CSV:
        FileBasedClient.validate(database)
    elif database.database_type == DatabaseType.EXCEL:
        FileBasedClient.validate(database)
    elif database.database_type == DatabaseType.PARQUET:
        FileBasedClient.validate(database)
    else:
        raise Exception("Unsupported database type! " + database.database_type)


def preview_tables(database_type: str, uploaded_file: UploadedFile) -> List[TablePreview]:
    if database_type == DatabaseType.CSV:
        return CSVClient.preview_tables(uploaded_file)
    elif database_type == DatabaseType.EXCEL:
        return ExcelClient.preview_tables(uploaded_file)
    return []


def preview_table(
    uploaded_file: UploadedFile, sanitized_orig_table_name: str, config: TableConfig
) -> TablePreview:
    return ExcelClient.preview_table(uploaded_file, sanitized_orig_table_name, config)


def fetch_schema(database: Database) -> DatabaseSchema:
    schema_client = None
    if database.database_type == DatabaseType.SNOWFLAKE:
        schema_client = SnowflakeSchemaClient(database)
    elif database.database_type == DatabaseType.BIGQUERY:
        schema_client = BigQuerySchemaClient(database)
    else:
        raise Exception(
            "Cannot fetch schema for database type: " + database.database_type
        )
    return schema_client.fetch().model_copy()
