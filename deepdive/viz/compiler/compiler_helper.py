from deepdive.viz.compiler.compiler import VizSpecCompiler
from deepdive.viz.compiler.sqlite_compiler import SqliteCompiler
from deepdive.viz.compiler.bigquery_complier import BigQueryCompiler
from deepdive.viz.compiler.snowflake_compiler import SnowflakeCompiler
from deepdive.schema import DatabaseSchema, SqlDialect


def get_compiler(db_schema: DatabaseSchema) -> VizSpecCompiler:
    if db_schema.sql_dialect == SqlDialect.SQLITE:
        return SqliteCompiler(db_schema)
    elif db_schema.sql_dialect == SqlDialect.GOOGLE_SQL:
        return BigQueryCompiler(db_schema)
    elif db_schema.sql_dialect == SqlDialect.SNOWFLAKE_SQL:
        return SnowflakeCompiler(db_schema)

    return SqliteCompiler(db_schema)
