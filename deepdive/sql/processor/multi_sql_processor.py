from typing import Tuple, Optional

from deepdive.sql.processor.sql_processor import SqlProcessor
from deepdive.sql.parser import SqlTree


class MultiSqlProcessor(SqlProcessor):
    def __init__(self, *args: Tuple[SqlProcessor]):
        self.processors = list(filter(None, args))

    def process(self, sql_tree: SqlTree) -> Optional[SqlTree]:
        for processor in self.processors:
            sql_tree = processor.process(sql_tree)

            if not sql_tree:
                return None
        return sql_tree
