from typing import Tuple

from deepdive.viz.processor.viz_spec_processor import VizSpecProcessor
from deepdive.schema import VizSpec


class MultiVizSpecProcessor(VizSpecProcessor):
    def __init__(self, *args: Tuple[VizSpecProcessor]):
        self.processors = list(filter(None, args))

    def process(self, viz_spec: VizSpec) -> VizSpec:
        for processor in self.processors:
            viz_spec = processor.process(viz_spec)

            if not viz_spec:
                return None
        return viz_spec
