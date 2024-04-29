from pypika import functions as fn
from pypika.enums import Arithmetic
from pypika.terms import ArithmeticExpression, Field, ValueWrapper, LiteralValue

from deepdive.schema import (
    Binner,
    Breakdown,
    Filter,
    SortBy,
    VizSpec,
    XAxis,
    YAxis,
    VizSpecError,
)
from deepdive.sql.parser.sql_tree import SqlTree
from deepdive.test.sql.sql_test_case import SqlTestCase
from deepdive.schema import DatabaseSchema, SqlDialect
from deepdive.viz.generator.bigquery_generator import BigQueryGenerator
from deepdive.viz.processor import NoopProcessor


DB_SCHEMA = DatabaseSchema(tables=[], sql_dialect=SqlDialect.GOOGLE_SQL)
GENERATOR = BigQueryGenerator(DB_SCHEMA, NoopProcessor())


def generate_viz_spec(sql_tree):
    return GENERATOR.generate(sql_tree)


class TestBigQueryGenerator(SqlTestCase):
    def test_select_datetime_x(self):
        self.assertEqual(
            VizSpec(
                x_axis=XAxis(
                    name="started_at",
                    binner=Binner(binner_type="datetime", time_unit="second"),
                ),
                tables=["customers"],
            ),
            generate_viz_spec(
                SqlTree(
                    select_terms=[
                        fn.Function(
                            "FORMAT_DATE", "%Y-%m-%d %H:%M:%S", Field("started_at")
                        )
                    ],
                    groupby_terms=[
                        fn.Function(
                            "FORMAT_DATE", "%Y-%m-%d %H:%M:%S", Field("started_at")
                        )
                    ],
                    from_term="customers",
                ),
            ),
        )

    def test_select_datetime_x_breakdown(self):
        self.assertEqual(
            VizSpec(
                x_axis=XAxis(
                    name="started_at",
                    binner=Binner(binner_type="datetime", time_unit="second"),
                ),
                breakdowns=[Breakdown(name="a")],
                tables=["customers"],
            ),
            generate_viz_spec(
                SqlTree(
                    select_terms=[
                        fn.Function(
                            "FORMAT_DATE", "%Y-%m-%d %H:%M:%S", Field("started_at")
                        )
                    ],
                    groupby_terms=[
                        Field("a"),
                        fn.Function(
                            "FORMAT_DATE", "%Y-%m-%d %H:%M:%S", Field("started_at")
                        ),
                    ],
                    from_term="customers",
                ),
            ),
        )

    def test_select_datetime_x_breakdown_out_of_order(self):
        self.assertEqual(
            VizSpec(
                x_axis=XAxis(
                    name="started_at",
                    binner=Binner(binner_type="datetime", time_unit="second"),
                ),
                breakdowns=[Breakdown(name="a")],
                tables=["customers"],
            ),
            generate_viz_spec(
                SqlTree(
                    select_terms=[
                        fn.Function(
                            "FORMAT_DATE", "%Y-%m-%d %H:%M:%S", Field("started_at")
                        )
                    ],
                    groupby_terms=[
                        fn.Function(
                            "FORMAT_DATE", "%Y-%m-%d %H:%M:%S", Field("started_at")
                        ),
                        Field("a"),
                    ],
                    from_term="customers",
                ),
            ),
        )

    def test_select_datetime_x_alias(self):
        self.assertEqual(
            VizSpec(
                x_axis=XAxis(
                    name="started_at",
                    binner=Binner(binner_type="datetime", time_unit="second"),
                    alias="started_at_seconds",
                ),
                tables=["customers"],
            ),
            generate_viz_spec(
                SqlTree(
                    select_terms=[
                        fn.Function(
                            "FORMAT_DATE", "%Y-%m-%d %H:%M:%S", Field("started_at")
                        ).as_("started_at_seconds")
                    ],
                    groupby_terms=[Field("started_at_seconds")],
                    from_term="customers",
                ),
            ),
        )

    def test_select_datetime_x_minutes(self):
        self.assertEqual(
            VizSpec(
                x_axis=XAxis(
                    name="started_at",
                    binner=Binner(binner_type="datetime", time_unit="minute"),
                ),
                tables=["customers"],
            ),
            generate_viz_spec(
                SqlTree(
                    select_terms=[
                        fn.Function(
                            "FORMAT_DATE", "%Y-%m-%d %H:%M", Field("started_at")
                        )
                    ],
                    groupby_terms=[
                        fn.Function(
                            "FORMAT_DATE", "%Y-%m-%d %H:%M", Field("started_at")
                        )
                    ],
                    from_term="customers",
                ),
            ),
        )

    def test_select_datetime_x_hours(self):
        self.assertEqual(
            VizSpec(
                x_axis=XAxis(
                    name="started_at",
                    binner=Binner(binner_type="datetime", time_unit="hour"),
                ),
                tables=["customers"],
            ),
            generate_viz_spec(
                SqlTree(
                    select_terms=[
                        fn.Function("FORMAT_DATE", "%Y-%m-%d %H", Field("started_at"))
                    ],
                    groupby_terms=[
                        fn.Function("FORMAT_DATE", "%Y-%m-%d %H", Field("started_at"))
                    ],
                    from_term="customers",
                ),
            ),
        )

    def test_select_datetime_x_days(self):
        self.assertEqual(
            VizSpec(
                x_axis=XAxis(
                    name="started_at",
                    binner=Binner(binner_type="datetime", time_unit="day"),
                ),
                tables=["customers"],
            ),
            generate_viz_spec(
                SqlTree(
                    select_terms=[
                        fn.Function("FORMAT_DATE", "%Y-%m-%d", Field("started_at"))
                    ],
                    groupby_terms=[
                        fn.Function("FORMAT_DATE", "%Y-%m-%d", Field("started_at"))
                    ],
                    from_term="customers",
                ),
            ),
        )

    def test_select_datetime_x_months(self):
        self.assertEqual(
            VizSpec(
                x_axis=XAxis(
                    name="started_at",
                    binner=Binner(binner_type="datetime", time_unit="month"),
                ),
                tables=["customers"],
            ),
            generate_viz_spec(
                SqlTree(
                    select_terms=[
                        fn.Function("FORMAT_DATE", "%Y-%m", Field("started_at"))
                    ],
                    groupby_terms=[
                        fn.Function("FORMAT_DATE", "%Y-%m", Field("started_at"))
                    ],
                    from_term="customers",
                ),
            ),
        )

    def test_select_datetime_x_years(self):
        self.assertEqual(
            VizSpec(
                x_axis=XAxis(
                    name="started_at",
                    binner=Binner(binner_type="datetime", time_unit="year"),
                ),
                tables=["customers"],
            ),
            generate_viz_spec(
                SqlTree(
                    select_terms=[
                        fn.Function("FORMAT_DATE", "%Y", Field("started_at"))
                    ],
                    groupby_terms=[
                        fn.Function("FORMAT_DATE", "%Y", Field("started_at"))
                    ],
                    from_term="customers",
                ),
            ),
        )

    def test_select_date_func(self):
        self.assertEqual(
            VizSpec(
                x_axis=XAxis(
                    name="started_at",
                    binner=Binner(binner_type="datetime", time_unit="day"),
                ),
                tables=["customers"],
            ),
            generate_viz_spec(
                SqlTree(
                    select_terms=[fn.Function("DATE", Field("started_at"))],
                    groupby_terms=[fn.Function("DATE", Field("started_at"))],
                    from_term="customers",
                ),
            ),
        )

    def test_select_date_trunc(self):
        self.assertEqual(
            VizSpec(
                x_axis=XAxis(
                    name="started_at",
                    binner=Binner(binner_type="datetime", time_unit="week"),
                ),
                tables=["customers"],
            ),
            generate_viz_spec(
                SqlTree(
                    select_terms=[
                        fn.Function("DATE_TRUNC", Field("started_at"), "WEEK")
                    ],
                    groupby_terms=[
                        fn.Function("DATE_TRUNC", Field("started_at"), "WEEK")
                    ],
                    from_term="customers",
                ),
            ),
        )

    def test_select_date_trunc_day(self):
        self.assertEqual(
            VizSpec(
                x_axis=XAxis(
                    name="started_at",
                    binner=Binner(binner_type="datetime", time_unit="day"),
                ),
                tables=["customers"],
            ),
            generate_viz_spec(
                SqlTree(
                    select_terms=[
                        fn.Function("DATE_TRUNC", Field("started_at"), "DAY")
                    ],
                    groupby_terms=[
                        fn.Function("DATE_TRUNC", Field("started_at"), "DAY")
                    ],
                    from_term="customers",
                ),
            ),
        )

    def test_select_date_trunc_month(self):
        self.assertEqual(
            VizSpec(
                x_axis=XAxis(
                    name="started_at",
                    binner=Binner(binner_type="datetime", time_unit="month"),
                ),
                tables=["customers"],
            ),
            generate_viz_spec(
                SqlTree(
                    select_terms=[
                        fn.Function("DATE_TRUNC", Field("started_at"), "MONTH")
                    ],
                    groupby_terms=[
                        fn.Function("DATE_TRUNC", Field("started_at"), "MONTH")
                    ],
                    from_term="customers",
                ),
            ),
        )

    def test_select_date_trunc_month_swapped(self):
        self.assertEqual(
            VizSpec(
                x_axis=XAxis(
                    name="started_at",
                    binner=Binner(binner_type="datetime", time_unit="month"),
                ),
                tables=["customers"],
            ),
            generate_viz_spec(
                SqlTree(
                    select_terms=[
                        fn.Function("DATE_TRUNC", "MONTH", Field("started_at"))
                    ],
                    groupby_terms=[
                        fn.Function("DATE_TRUNC", "MONTH", Field("started_at"))
                    ],
                    from_term="customers",
                ),
            ),
        )

    def test_select_date_extract(self):
        self.assertEqual(
            VizSpec(
                x_axis=XAxis(
                    name="started_at",
                    binner=Binner(binner_type="datetime", time_unit="month_of_year"),
                ),
                tables=["customers"],
            ),
            generate_viz_spec(
                SqlTree(
                    select_terms=[fn.Extract("MONTH", Field("started_at"))],
                    groupby_terms=[fn.Extract("MONTH", Field("started_at"))],
                    from_term="customers",
                ),
            ),
        )

    def test_select_date_extract_day(self):
        self.assertEqual(
            VizSpec(
                x_axis=XAxis(
                    name="started_at",
                    binner=Binner(binner_type="datetime", time_unit="day_of_month"),
                ),
                tables=["customers"],
            ),
            generate_viz_spec(
                SqlTree(
                    select_terms=[fn.Extract("DAY", Field("started_at"))],
                    groupby_terms=[fn.Extract("DAY", Field("started_at"))],
                    from_term="customers",
                ),
            ),
        )

    def test_select_date_extract_week(self):
        self.assertEqual(
            VizSpec(
                x_axis=XAxis(
                    name="started_at",
                    binner=Binner(binner_type="datetime", time_unit="week_of_year"),
                ),
                tables=["customers"],
            ),
            generate_viz_spec(
                SqlTree(
                    select_terms=[fn.Extract("WEEK", Field("started_at"))],
                    groupby_terms=[fn.Extract("WEEK", Field("started_at"))],
                    from_term="customers",
                ),
            ),
        )
