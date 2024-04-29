from typing import Optional
from deepdive.sql.processor.sql_processor import SqlProcessor
from deepdive.sql.parser import SqlTree

DEFAULT_LIMIT = 10000


class LimitProcessor(SqlProcessor):
    """
    Appends a LIMIT parameter to queries if not present

    The logic here supports sub-queries (i.e, will still append even if a subquery has a limit)
    """

    def __init__(self, limit=DEFAULT_LIMIT):
        self.limit = limit

    def process(self, sql_tree: SqlTree) -> Optional[SqlTree]:
        if not sql_tree.limit_term:
            sql_tree.limit_term = self.limit
        return sql_tree
