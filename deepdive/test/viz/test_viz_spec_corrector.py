import unittest
from deepdive.schema import XAxis, YAxis, Breakdown, VizSpec, VizSpecError
from deepdive.viz.parser import parse_spec


class TestVizSpecCorrector(unittest.TestCase):
    def test_correct_y_axises(self):
        viz_spec_string = """
        {
            "x_axis": {"name": "a"},
            "y_axises": [{"name": "b"}],
            "breakdowns": [],
            "filters": []
        }
        """

        self.assertEqual(
            VizSpec(
                x_axis=XAxis(name="a"), y_axises=[YAxis(name="*", aggregation="COUNT")]
            ),
            parse_spec(viz_spec_string),
        )

    def test_correct_y_axises_sort_existing(self):
        viz_spec_string = """
        {
            "x_axis": {"name": "a"},
            "y_axises": [{"name": "b"}],
            "breakdowns": [],
            "filters": [],
            "sort_by": {"name": "b"}
        }
        """

        self.assertEqual(
            VizSpec(
                x_axis=XAxis(name="a"), y_axises=[YAxis(name="*", aggregation="COUNT")]
            ),
            parse_spec(viz_spec_string),
        )

    def test_correct_y_axises_breakdown(self):
        viz_spec_string = """
        {
            "y_axises": [{"name": "b"}],
            "breakdowns": [{"name": "a"}],
            "filters": []
        }
        """

        self.assertEqual(
            VizSpec(
                y_axises=[YAxis(name="*", aggregation="COUNT")],
                breakdowns=[Breakdown(name="a")],
            ),
            parse_spec(viz_spec_string),
        )

    def test_correct_missing_sort_by(self):
        viz_spec_string = """
        {
            "x_axis": {"name": "a"},
            "y_axises": [],
            "breakdowns": [],
            "filters": [],
            "sort_by": {"name": "b", "direction": "asc"}
        }
        """

        self.assertEqual(
            VizSpec(x_axis=XAxis(name="a")),
            parse_spec(viz_spec_string),
        )

    def test_correct_star_extra(self):
        viz_spec_string = """
        {
            "y_axises": [{"name": "*"}, {"name": "b"}],
            "breakdowns": [],
            "filters": []
        }
        """

        self.assertEqual(
            VizSpec(y_axises=[YAxis(name="b")]),
            parse_spec(viz_spec_string),
        )

    def test_correct_star_extra_2(self):
        viz_spec_string = """
        {
            "y_axises": [{"name": "*"}, {"name": "b"}, {"name": "c"}],
            "breakdowns": [],
            "filters": []
        }
        """

        self.assertEqual(
            VizSpec(y_axises=[YAxis(name="b"), YAxis(name="c")]),
            parse_spec(viz_spec_string),
        )
