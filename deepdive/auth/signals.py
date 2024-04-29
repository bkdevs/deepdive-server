from pathlib import Path

import json
import pandas as pd
from allauth.account.signals import user_signed_up
from django.core.files.storage import default_storage
from django.dispatch import receiver

from deepdive.database.file_based_client_helper import (
    create_table_schema,
    sanitize_table_name,
)
from deepdive.models import Database, DatabaseFile, DatabaseType
from deepdive.schema import DatabaseSchema, SqlDialect, TableConfig

# REPLACE THIS
DEFAULT_SNOWFLAKE_DATABASE_CONFIG = {
    "database_type": DatabaseType.SNOWFLAKE,
    "name": "Demo Snowflake DB",
    "username": "REDACTED",
    "password": "REDACTED",
    "snowflake_account": "REDACTED",
    "snowflake_database": "SNOWFLAKE_SAMPLE_DATA",
    "snowflake_schema": "TPCH_SF1",
    "starter_questions": [
        "Chart orders by month",
        "Get top performing segments",
        "How many customers do I have by nation?",
    ],
}

DEFAULT_SNOWFLAKE_DB_SCHEMA = {
    "tables": [
        {
            "name": "CUSTOMER",
            "columns": [
                {"name": "C_CUSTKEY", "column_type": "int"},
                {"name": "C_NAME", "column_type": "text"},
                {"name": "C_ADDRESS", "column_type": "text"},
                {"name": "C_NATIONKEY", "column_type": "int"},
                {"name": "C_PHONE", "column_type": "text"},
                {"name": "C_ACCTBAL", "column_type": "float"},
                {"name": "C_MKTSEGMENT", "column_type": "text"},
                {"name": "C_COMMENT", "column_type": "text"},
            ],
        },
        {
            "name": "LINEITEM",
            "columns": [
                {"name": "L_ORDERKEY", "column_type": "int"},
                {"name": "L_PARTKEY", "column_type": "int"},
                {"name": "L_SUPPKEY", "column_type": "int"},
                {"name": "L_LINENUMBER", "column_type": "int"},
                {"name": "L_QUANTITY", "column_type": "float"},
                {"name": "L_EXTENDEDPRICE", "column_type": "float"},
                {"name": "L_DISCOUNT", "column_type": "float"},
                {"name": "L_TAX", "column_type": "float"},
                {"name": "L_RETURNFLAG", "column_type": "text"},
                {"name": "L_LINESTATUS", "column_type": "text"},
                {"name": "L_SHIPDATE", "column_type": "date"},
                {"name": "L_COMMITDATE", "column_type": "date"},
                {"name": "L_RECEIPTDATE", "column_type": "date"},
                {"name": "L_SHIPINSTRUCT", "column_type": "text"},
                {"name": "L_SHIPMODE", "column_type": "text"},
                {"name": "L_COMMENT", "column_type": "text"},
            ],
        },
        {
            "name": "NATION",
            "columns": [
                {"name": "N_NATIONKEY", "column_type": "int"},
                {"name": "N_NAME", "column_type": "text"},
                {"name": "N_REGIONKEY", "column_type": "int"},
                {"name": "N_COMMENT", "column_type": "text"},
            ],
        },
        {
            "name": "ORDERS",
            "columns": [
                {"name": "O_ORDERKEY", "column_type": "int"},
                {"name": "O_CUSTKEY", "column_type": "int"},
                {"name": "O_ORDERSTATUS", "column_type": "text"},
                {"name": "O_TOTALPRICE", "column_type": "float"},
                {"name": "O_ORDERDATE", "column_type": "date"},
                {"name": "O_ORDERPRIORITY", "column_type": "text"},
                {"name": "O_CLERK", "column_type": "text"},
                {"name": "O_SHIPPRIORITY", "column_type": "int"},
                {"name": "O_COMMENT", "column_type": "text"},
            ],
        },
        {
            "name": "PART",
            "columns": [
                {"name": "P_PARTKEY", "column_type": "int"},
                {"name": "P_NAME", "column_type": "text"},
                {"name": "P_MFGR", "column_type": "text"},
                {"name": "P_BRAND", "column_type": "text"},
                {"name": "P_TYPE", "column_type": "text"},
                {"name": "P_SIZE", "column_type": "int"},
                {"name": "P_CONTAINER", "column_type": "text"},
                {"name": "P_RETAILPRICE", "column_type": "float"},
                {"name": "P_COMMENT", "column_type": "text"},
            ],
        },
        {
            "name": "PARTSUPP",
            "columns": [
                {"name": "PS_PARTKEY", "column_type": "int"},
                {"name": "PS_SUPPKEY", "column_type": "int"},
                {"name": "PS_AVAILQTY", "column_type": "int"},
                {"name": "PS_SUPPLYCOST", "column_type": "float"},
                {"name": "PS_COMMENT", "column_type": "text"},
            ],
        },
        {
            "name": "REGION",
            "columns": [
                {"name": "R_REGIONKEY", "column_type": "int"},
                {"name": "R_NAME", "column_type": "text"},
                {"name": "R_COMMENT", "column_type": "text"},
            ],
        },
        {
            "name": "SUPPLIER",
            "columns": [
                {"name": "S_SUPPKEY", "column_type": "int"},
                {"name": "S_NAME", "column_type": "text"},
                {"name": "S_ADDRESS", "column_type": "text"},
                {"name": "S_NATIONKEY", "column_type": "int"},
                {"name": "S_PHONE", "column_type": "text"},
                {"name": "S_ACCTBAL", "column_type": "float"},
                {"name": "S_COMMENT", "column_type": "text"},
            ],
        },
    ],
    "primary_keys": [],
    "foreign_keys": [
        {"primary": "CUSTOMER.C_CUSTKEY", "reference": "ORDERS.O_CUSTKEY"},
        {"primary": "CUSTOMER.C_NATIONKEY", "reference": "NATION.N_NATIONKEY"},
        {"primary": "LINEITEM.L_ORDERKEY", "reference": "ORDERS.O_ORDERKEY"},
        {"primary": "LINEITEM.L_PARTKEY", "reference": "PART.P_PARTKEY"},
        {"primary": "LINEITEM.L_SUPPKEY", "reference": "SUPPLIER.S_SUPPKEY"},
        {"primary": "NATION.N_NATIONKEY", "reference": "SUPPLIER.S_NATIONKEY"},
        {"primary": "NATION.N_REGIONKEY", "reference": "REGION.R_REGIONKEY"},
        {"primary": "PART.P_PARTKEY", "reference": "PARTSUPP.PS_PARTKEY"},
        {"primary": "PARTSUPP.PS_SUPPKEY", "reference": "SUPPLIER.S_SUPPKEY"},
    ],
    "sql_dialect": "Snowflake",
}

DEFAULT_BIGQUERY_DATABASE_CONFIG = {
    "database_type": DatabaseType.BIGQUERY,
    "name": "Demo BigQuery DB",
    "bigquery_dataset_id": "bigquery-public-data.austin_bikeshare",
    "starter_questions": [
        "Top 10 most popular stations and names",
        "Bike subscriptions by type",
        "Rides by hour of day",
    ],
}

DEFAULT_BIGQUERY_DB_SCHEMA = {
    "tables": [
        {
            "name": "bikeshare_stations",
            "columns": [
                {"name": "station_id", "column_type": "int", "comment": "None"},
                {"name": "name", "column_type": "text", "comment": "None"},
                {"name": "status", "column_type": "text", "comment": "None"},
                {"name": "address", "column_type": "text", "comment": "None"},
                {"name": "alternate_name", "column_type": "text", "comment": "None"},
                {"name": "city_asset_number", "column_type": "int", "comment": "None"},
                {"name": "property_type", "column_type": "text", "comment": "None"},
                {"name": "number_of_docks", "column_type": "int", "comment": "None"},
                {"name": "power_type", "column_type": "text", "comment": "None"},
                {"name": "footprint_length", "column_type": "int", "comment": "None"},
                {"name": "footprint_width", "column_type": "float", "comment": "None"},
                {"name": "notes", "column_type": "text", "comment": "None"},
                {"name": "council_district", "column_type": "int", "comment": "None"},
                {"name": "modified_date", "column_type": "time", "comment": "None"},
            ],
        },
        {
            "name": "bikeshare_trips",
            "columns": [
                {
                    "name": "trip_id",
                    "column_type": "text",
                    "comment": "Numeric ID of bike trip",
                },
                {
                    "name": "subscriber_type",
                    "column_type": "text",
                    "comment": "Type of the Subscriber",
                },
                {
                    "name": "bike_id",
                    "column_type": "text",
                    "comment": "ID of bike used",
                },
                {
                    "name": "bike_type",
                    "column_type": "text",
                    "comment": "Type of bike used",
                },
                {
                    "name": "start_time",
                    "column_type": "time",
                    "comment": "Start timestamp of trip",
                },
                {
                    "name": "start_station_id",
                    "column_type": "int",
                    "comment": "Numeric reference for start station",
                },
                {
                    "name": "start_station_name",
                    "column_type": "text",
                    "comment": "Station name for start station",
                },
                {
                    "name": "end_station_id",
                    "column_type": "text",
                    "comment": "Numeric reference for end station",
                },
                {
                    "name": "end_station_name",
                    "column_type": "text",
                    "comment": "Station name for end station",
                },
                {
                    "name": "duration_minutes",
                    "column_type": "int",
                    "comment": "Time of trip in minutes",
                },
            ],
        },
    ],
    "primary_keys": [],
    "foreign_keys": [
        {
            "primary": "bikeshare_stations.station_id",
            "reference": "bikeshare_trips.end_station_id",
        },
        {
            "primary": "bikeshare_stations.station_id",
            "reference": "bikeshare_trips.start_station_id",
        },
    ],
    "sql_dialect": "GoogleSQL",
}

DEFAULT_EXCEL_S3_PATH = "default/employee_data.xlsx"
DEFAULT_EXCEL_DATABASE_CONFIG = {
    "database_type": DatabaseType.EXCEL,
    "name": "Demo Excel DB",
    "starter_questions": [
        "How many employees are there in each department?",
        "What is the average annual salary by department?",
        "What is the distribution of employees by ethnicity?",
    ],
}

# insurance against us making backwards incompatible changes and breaking signup
DatabaseSchema.model_validate(DEFAULT_SNOWFLAKE_DB_SCHEMA)
DatabaseSchema.model_validate(DEFAULT_BIGQUERY_DB_SCHEMA)


@receiver(user_signed_up)
def add_default_databases(sender, **kwargs):
    new_user = kwargs["user"]
    add_default_database(
        new_user, DEFAULT_SNOWFLAKE_DATABASE_CONFIG, DEFAULT_SNOWFLAKE_DB_SCHEMA
    )
    add_default_database(
        new_user, DEFAULT_BIGQUERY_DATABASE_CONFIG, DEFAULT_BIGQUERY_DB_SCHEMA
    )
    add_default_csv_database(new_user)


def add_default_database(user, db_config, db_schema_json):
    default_database = Database(user=user, **db_config)
    default_database.schema = json.dumps(db_schema_json)
    default_database.save()


def add_default_csv_database(user):
    with default_storage.open(DEFAULT_EXCEL_S3_PATH) as default_excel:
        default_database = Database(user=user, **DEFAULT_EXCEL_DATABASE_CONFIG)
        excel_file = pd.ExcelFile(default_excel)
        table_schemas = []
        table_configs = {}
        for sheet_name in excel_file.sheet_names:
            sanitized_sheet_name = sanitize_table_name(sheet_name)
            table_schema = create_table_schema(
                sanitized_sheet_name,
                excel_file.parse(sheet_name=sheet_name, nrows=1),
            )
            table_schemas.append(table_schema)
            table_configs[sanitized_sheet_name] = TableConfig(
                name=sanitized_sheet_name
            ).model_dump()
        db_schema = DatabaseSchema(tables=table_schemas, sql_dialect=SqlDialect.SQLITE)
        default_database.schema = db_schema.model_dump_json(exclude_none=True)
        default_database.save()

        database_file = DatabaseFile(
            user=user,
            database=default_database,
            file=default_excel,
            configs=json.dumps(table_configs),
        )
        database_file.save()
