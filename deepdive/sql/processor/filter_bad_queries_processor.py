from typing import Optional
from deepdive.sql.processor.sql_processor import SqlProcessor
from deepdive.schema import DatabaseSchema
from deepdive.sql.parser import SqlTree


class FilterBadQueriesProcessor(SqlProcessor):
    """
    Filters invalid SQL queries generated
    """

    def __init__(self, db_schema: DatabaseSchema):
        self.db_schema = db_schema

    def process(self, sql_tree: SqlTree) -> Optional[SqlTree]:
        if not sql_tree.from_term:
            return None

        table_name = (
            sql_tree.from_term
            if isinstance(sql_tree.from_term, str)
            else sql_tree.from_term._table_name
        )

        if not self.db_schema.get_table(table_name):
            return None

        return sql_tree
