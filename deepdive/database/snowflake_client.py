import logging

from pandas import DataFrame
from snowflake.snowpark import Session

from deepdive.database.client import DatabaseClient
from deepdive.database.snowflake_helper import infer_missing_dtypes
from deepdive.models import Database

logger = logging.getLogger(__name__)


class SnowflakeClient(DatabaseClient):
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

    def validate(database: Database):
        Session.builder.configs(
            {
                "user": database.username,
                "password": database.password,
                "account": database.snowflake_account,
                "database": database.snowflake_database,
                "schema": database.snowflake_schema,
            }
        ).create()

    def finalize(self):
        pass

    def execute_query(self, query: str) -> DataFrame:
        snowpark_df = self.session.sql(query)
        pandas_df = snowpark_df.to_pandas()
        return infer_missing_dtypes(snowpark_df, pandas_df)
