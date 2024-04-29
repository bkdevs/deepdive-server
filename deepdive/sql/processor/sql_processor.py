from typing import Optional
from abc import ABC, abstractmethod
from deepdive.sql.parser import SqlTree


class SqlProcessor(ABC):
    @abstractmethod
    def process(self, sql_tree: SqlTree) -> Optional[SqlTree]:
        pass
