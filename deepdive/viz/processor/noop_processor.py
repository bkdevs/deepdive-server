from deepdive.schema import VizSpec
from deepdive.viz.processor.viz_spec_processor import VizSpecProcessor


class NoopProcessor(VizSpecProcessor):
    def process(self, viz_spec: VizSpec) -> VizSpec:
        return viz_spec
