import unittest

from deepdive.schema import VizSpecError
from deepdive.schema import Breakdown, SortBy, VizSpec, XAxis, YAxis


class TestVizSpecValidator(unittest.TestCase):
    def test_model_validate_throws(self):
        with self.assertRaises(VizSpecError):
            VizSpec.model_validate(
                {"x_axis": {"name": "a"}, "y_axises": [{"name": "b"}]}
            )

    def test_unaggregated_y_axises(self):
        with self.assertRaises(VizSpecError):
            VizSpec(x_axis=XAxis(name="a"), y_axises=[YAxis(name="b")])

    def test_unaggregated_y_axises_star(self):
        with self.assertRaises(VizSpecError):
            VizSpec(x_axis=XAxis(name="a"), y_axises=[YAxis(name="*")])

    def test_aggregated_y_axises(self):
        VizSpec(x_axis=XAxis(name="a"), y_axises=[YAxis(name="b", aggregation="SUM")])

    def test_invalid_sort_by(self):
        with self.assertRaises(VizSpecError):
            VizSpec(
                x_axis=XAxis(name="a"),
                y_axises=[YAxis(name="b", aggregation="COUNT")],
                sort_by=SortBy(name="c"),
            )

    def test_sort_by_okay_x(self):
        VizSpec(
            x_axis=XAxis(name="a"),
            y_axises=[YAxis(name="b", aggregation="COUNT")],
            sort_by=SortBy(name="a"),
        )

    def test_sort_by_okay_y(self):
        VizSpec(
            x_axis=XAxis(name="a"),
            y_axises=[YAxis(name="b", aggregation="COUNT")],
            sort_by=SortBy(name="b"),
        )

    def test_sort_by_random_okay_star(self):
        VizSpec(
            y_axises=[YAxis(name="*")],
            sort_by=SortBy(name="b"),
        )

    def test_sort_by_okay_breakdown(self):
        VizSpec(
            x_axis=XAxis(name="a"),
            y_axises=[YAxis(name="b", aggregation="COUNT")],
            breakdowns=[Breakdown(name="c")],
            sort_by=SortBy(name="c"),
        )

    def test_duplicate_x_y_axis(self):
        with self.assertRaises(VizSpecError):
            VizSpec(x_axis=XAxis(name="a"), y_axises=[YAxis(name="a")])

    def test_duplicate_x_breakdown(self):
        with self.assertRaises(VizSpecError):
            VizSpec(x_axis=XAxis(name="a"), breakdowns=[Breakdown(name="a")])

    def test_duplicate_x_extra_star(self):
        with self.assertRaises(VizSpecError):
            VizSpec(y_axises=[YAxis(name="*"), YAxis(name="age")])

    def test_duplicate_x_extra_star_aggregated(self):
        VizSpec(y_axises=[YAxis(name="*", aggregation="COUNT"), YAxis(name="age")])
