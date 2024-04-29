from abc import ABC, abstractmethod
from deepdive.schema import VizSpec


class VizSpecProcessor(ABC):
    @abstractmethod
    def process(self, viz_spec: VizSpec) -> VizSpec:
        pass
