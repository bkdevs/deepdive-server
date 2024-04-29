from typing import Optional
from deepdive.schema import VizSpec
from deepdive.sql.parser import SqlTree

from abc import ABC, abstractmethod


class VizSpecGenerator(ABC):
    @abstractmethod
    def generate(sql_tree: SqlTree) -> Optional[VizSpec]:
        pass
