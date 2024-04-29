from deepdive.viz.generator.generator import VizSpecGenerator
from deepdive.viz.generator.sqlite_generator import SqliteGenerator
from deepdive.viz.generator.bigquery_generator import BigQueryGenerator
from deepdive.viz.generator.snowflake_generator import SnowflakeGenerator
from deepdive.schema import DatabaseSchema, SqlDialect
from deepdive.viz.processor import VizTypeProcessor


def get_generator(db_schema: DatabaseSchema) -> VizSpecGenerator:
    if db_schema.sql_dialect == SqlDialect.SQLITE:
        return SqliteGenerator(db_schema, VizTypeProcessor(db_schema))
    elif db_schema.sql_dialect == SqlDialect.GOOGLE_SQL:
        return BigQueryGenerator(db_schema, VizTypeProcessor(db_schema))
    elif db_schema.sql_dialect == SqlDialect.SNOWFLAKE_SQL:
        return SnowflakeGenerator(db_schema, VizTypeProcessor(db_schema))

    return SqliteGenerator(db_schema, VizTypeProcessor(db_schema))
