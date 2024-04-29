import unittest

from deepdive.schema import *
from deepdive.viz.processor.alias_processor import AliasProcessor
from deepdive.viz.processor.multi_viz_spec_processor import MultiVizSpecProcessor


class TestMultiVizSpecProcessor(unittest.TestCase):
    def setUp(self):
        self.processor = MultiVizSpecProcessor(AliasProcessor())

    def test_no_change_for_unaggregated(self):
        viz_spec = VizSpec(y_axises=[YAxis(name="b"), YAxis(name="c")])
        self.assertEqual(viz_spec, self.processor.process(viz_spec))

    def test_change_for_aggregated(self):
        viz_spec = VizSpec(
            x_axis=XAxis(name="a"),
            y_axises=[YAxis(name="b", aggregation="SUM")],
        )
        expected = VizSpec(
            x_axis=XAxis(name="a"),
            y_axises=[
                YAxis(name="b", aggregation="SUM", alias="SUM(b)"),
            ],
        )
        self.assertEqual(expected, self.processor.process(viz_spec))
