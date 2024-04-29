from typing import Optional
from deepdive.schema import VizSpec
from deepdive.sql.parser import SqlTree

from abc import ABC, abstractmethod


class VizSpecCompiler(ABC):
    @abstractmethod
    def compile(viz_spec: VizSpec) -> Optional[SqlTree]:
        pass
