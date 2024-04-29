import unittest

from deepdive.schema import *
from deepdive.viz.processor.alias_processor import AliasProcessor


class TestAliasProcessor(unittest.TestCase):
    def setUp(self):
        self.processor = AliasProcessor()

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
                YAxis(name="b", aggregation="SUM", alias="SUM_b"),
            ],
        )
        self.assertEqual(expected, self.processor.process(viz_spec))

    def test_change_for_aggregated_count_star(self):
        viz_spec = VizSpec(
            x_axis=XAxis(name="a"),
            y_axises=[YAxis(name="*", aggregation="COUNT")],
        )
        expected = VizSpec(
            x_axis=XAxis(name="a"),
            y_axises=[
                YAxis(name="*", aggregation="COUNT", alias="COUNT_ROWS"),
            ],
        )
        self.assertEqual(expected, self.processor.process(viz_spec))

    def test_change_for_existing_alias(self):
        viz_spec = VizSpec(
            x_axis=XAxis(name="a"),
            y_axises=[YAxis(name="b", aggregation="SUM", alias="some_alias")],
        )
        expected = VizSpec(
            x_axis=XAxis(name="a"),
            y_axises=[
                YAxis(name="b", aggregation="SUM", alias="SUM_b"),
            ],
        )
        self.assertEqual(expected, self.processor.process(viz_spec))

    def test_change_for_aggregated_many(self):
        viz_spec = VizSpec(
            x_axis=XAxis(name="a"),
            y_axises=[
                YAxis(name="b", aggregation="SUM"),
                YAxis(name="c", aggregation="COUNT"),
            ],
        )
        expected = VizSpec(
            x_axis=XAxis(name="a"),
            y_axises=[
                YAxis(name="b", aggregation="SUM", alias="SUM_b"),
                YAxis(name="c", aggregation="COUNT", alias="COUNT_c"),
            ],
        )
        self.assertEqual(expected, self.processor.process(viz_spec))

    def test_change_for_aggregated_many(self):
        viz_spec = VizSpec(
            x_axis=XAxis(name="a"),
            y_axises=[
                YAxis(name="b", aggregation="SUM"),
                YAxis(name="c", aggregation="COUNT"),
            ],
        )
        expected = VizSpec(
            x_axis=XAxis(name="a"),
            y_axises=[
                YAxis(name="b", aggregation="SUM", alias="SUM_b"),
                YAxis(name="c", aggregation="COUNT", alias="COUNT_c"),
            ],
        )
        self.assertEqual(expected, self.processor.process(viz_spec))

    def test_change_for_x_axis_binned(self):
        viz_spec = VizSpec(
            x_axis=XAxis(
                name="a", binner=Binner(binner_type="datetime", time_unit="day")
            ),
        )
        expected = VizSpec(
            x_axis=XAxis(
                name="a",
                alias="a_DAY",
                binner=Binner(binner_type="datetime", time_unit="day"),
            ),
        )
        self.assertEqual(expected, self.processor.process(viz_spec))

    def test_change_for_x_axis_binned_existing(self):
        viz_spec = VizSpec(
            x_axis=XAxis(
                name="a",
                binner=Binner(binner_type="datetime", time_unit="day"),
                alias="something_here",
            ),
        )
        expected = VizSpec(
            x_axis=XAxis(
                name="a",
                alias="a_DAY",
                binner=Binner(binner_type="datetime", time_unit="day"),
            ),
        )
        self.assertEqual(expected, self.processor.process(viz_spec))

    def test_change_for_y_axis_unparsed(self):
        viz_spec = VizSpec(
            x_axis=XAxis(
                name="a", binner=Binner(binner_type="datetime", time_unit="day")
            ),
            y_axises=[
                YAxis(
                    name="COUNT(*) * 100 / (select COUNT(*) from ORDERS)",
                    unparsed=True,
                )
            ],
        )
        expected = VizSpec(
            x_axis=XAxis(
                name="a",
                alias="a_DAY",
                binner=Binner(binner_type="datetime", time_unit="day"),
            ),
            y_axises=[
                YAxis(
                    name="COUNT(*) * 100 / (select COUNT(*) from ORDERS)",
                    unparsed=True,
                    alias="computed_column_1",
                )
            ],
        )
        self.assertEqual(expected, self.processor.process(viz_spec))

    def test_no_change_for_y_axis_unparsed_aggregated(self):
        viz_spec = VizSpec(
            y_axises=[
                YAxis(
                    name="COUNT(*) * 100 / (select COUNT(*) from ORDERS)",
                    unparsed=True,
                    alias="count_percentage",
                )
            ],
        )
        expected = VizSpec(
            y_axises=[
                YAxis(
                    name="COUNT(*) * 100 / (select COUNT(*) from ORDERS)",
                    unparsed=True,
                    alias="count_percentage",
                )
            ],
        )
        self.assertEqual(expected, self.processor.process(viz_spec))

    def test_unparsed_x_axis(self):
        viz_spec = VizSpec(
            x_axis=XAxis(name="COUNT(*) / 100", unparsed=True),
        )
        expected = VizSpec(
            x_axis=XAxis(name="COUNT(*) / 100", alias="computed_x_axis", unparsed=True),
        )
        self.assertEqual(expected, self.processor.process(viz_spec))

    def test_unparsed_x_axis_alias(self):
        viz_spec = VizSpec(
            x_axis=XAxis(name="COUNT(*) / 100", alias="month", unparsed=True),
        )
        expected = VizSpec(
            x_axis=XAxis(name="COUNT(*) / 100", alias="month", unparsed=True),
        )
        self.assertEqual(expected, self.processor.process(viz_spec))
