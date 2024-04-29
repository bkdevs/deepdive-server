from pypika import Table
from pypika import functions as fn
from pypika.enums import Arithmetic
from pypika.terms import ArithmeticExpression, Field, ValueWrapper

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
from deepdive.sql.parser.term_parser import UnparsedField
from deepdive.test.sql.sql_test_case import SqlTestCase, generate_viz_spec


class TestGenerator(SqlTestCase):
    """
    Tests that we can generate a VizSpec given a SqlTree
    """

    def test_simple_select(self):
        self.assertEqual(
            None,
            generate_viz_spec(
                SqlTree(from_term="customers"),
            ),
        )

    def test_simple_select_2(self):
        self.assertEqual(
            VizSpec(
                y_axises=[YAxis(name="a")],
                tables=["customers"],
            ),
            generate_viz_spec(
                SqlTree(select_terms=[Field("a")], from_term="customers"),
            ),
        )

    def test_simple_select_two_columns(self):
        self.assertEqual(
            VizSpec(
                y_axises=[YAxis(name="a"), YAxis(name="b")],
                tables=["customers"],
            ),
            generate_viz_spec(
                SqlTree(select_terms=[Field("a"), Field("b")], from_term="customers")
            ),
        )

    def test_simple_select_two_columns_orderby_one(self):
        self.assertEqual(
            VizSpec(
                y_axises=[YAxis(name="a"), YAxis(name="b")],
                tables=["customers"],
                sort_by=SortBy(name="a"),
            ),
            generate_viz_spec(
                SqlTree(
                    select_terms=[Field("a"), Field("b")],
                    from_term="customers",
                    orderby_term=(Field("a"), "ASC"),
                )
            ),
        )

    def test_simple_select_star(self):
        self.assertEqual(
            VizSpec(
                y_axises=[YAxis(name="*")],
                tables=["customers"],
            ),
            generate_viz_spec(SqlTree(select_terms=["*"], from_term="customers")),
        )

    def test_select_limit(self):
        self.assertEqual(
            VizSpec(
                y_axises=[YAxis(name="a")],
                tables=["customers"],
                limit=500,
            ),
            generate_viz_spec(
                SqlTree(
                    select_terms=[Field("a")], from_term="customers", limit_term=500
                )
            ),
        )

    def test_select_group_by(self):
        self.assertEqual(
            VizSpec(
                x_axis=XAxis(name="a"),
                tables=["customers"],
            ),
            generate_viz_spec(
                SqlTree(
                    select_terms=[Field("a")],
                    groupby_terms=[Field("a")],
                    from_term="customers",
                )
            ),
        )

    def test_select_group_by_alias(self):
        self.assertEqual(
            VizSpec(
                x_axis=XAxis(name="a", alias="b"),
                tables=["customers"],
            ),
            generate_viz_spec(
                SqlTree(
                    select_terms=[Field("a").as_("b")],
                    groupby_terms=[Field("b")],
                    from_term="customers",
                )
            ),
        )

    def test_select_group_by_xaxis(self):
        self.assertEqual(
            VizSpec(
                x_axis=XAxis(name="a"),
                tables=["customers"],
            ),
            generate_viz_spec(
                SqlTree(
                    select_terms=[Field("a")],
                    groupby_terms=[Field("a")],
                    from_term="customers",
                )
            ),
        )

    def test_select_group_by_xaxis_alias(self):
        self.assertEqual(
            VizSpec(
                x_axis=XAxis(name="a", alias="b"),
                tables=["customers"],
            ),
            generate_viz_spec(
                SqlTree(
                    select_terms=[Field("a").as_("b")],
                    groupby_terms=[Field("b")],
                    from_term="customers",
                )
            ),
        )

    def test_select_group_by_limit(self):
        self.assertEqual(
            VizSpec(
                x_axis=XAxis(name="a"),
                tables=["customers"],
                limit=100,
            ),
            generate_viz_spec(
                SqlTree(
                    select_terms=[Field("a")],
                    groupby_terms=[Field("a")],
                    from_term="customers",
                    limit_term=100,
                )
            ),
        )

    def test_raises_unaggregated_columns(self):
        # this is possible for GPT to generate, but rare(?) and not so useful
        # good question of what we should do in such scenarios
        with self.assertRaises(VizSpecError):
            generate_viz_spec(
                SqlTree(
                    select_terms=[Field("a"), Field("b")],
                    groupby_terms=[Field("a")],
                    from_term="customers",
                    limit_term=100,
                )
            ),

    def test_select_few_group_by_limit(self):
        self.assertEqual(
            VizSpec(
                y_axises=[YAxis(name="*", aggregation="COUNT")],
                x_axis=XAxis(name="a"),
                tables=["customers"],
                limit=100,
            ),
            generate_viz_spec(
                SqlTree(
                    select_terms=[Field("a"), fn.Count("*")],
                    groupby_terms=[Field("a")],
                    from_term="customers",
                    limit_term=100,
                )
            ),
        )

    def test_select_few_group_by_few_limit(self):
        self.assertEqual(
            VizSpec(
                x_axis=XAxis(name="a"),
                breakdowns=[Breakdown(name="b")],
                tables=["customers"],
                limit=100,
            ),
            generate_viz_spec(
                SqlTree(
                    select_terms=[Field("a"), Field("b")],
                    groupby_terms=[Field("a"), Field("b")],
                    from_term="customers",
                    limit_term=100,
                )
            ),
        )

    def test_select_aggregated_y(self):
        self.assertEqual(
            VizSpec(
                y_axises=[YAxis(name="*", aggregation="COUNT")],
                x_axis=XAxis(name="a"),
                breakdowns=[Breakdown(name="b")],
                tables=["customers"],
                limit=100,
            ),
            generate_viz_spec(
                SqlTree(
                    select_terms=[
                        fn.Count("*"),
                        Field("a"),
                        Field("b"),
                    ],
                    groupby_terms=[Field("a"), Field("b")],
                    from_term="customers",
                    limit_term=100,
                )
            ),
        )

    def test_select_aggregated_y_no_groupbys(self):
        self.assertEqual(
            VizSpec(
                y_axises=[YAxis(name="*", aggregation="COUNT")],
                x_axis=XAxis(name="a"),
                breakdowns=[Breakdown(name="b")],
                tables=["customers"],
                limit=100,
            ),
            generate_viz_spec(
                SqlTree(
                    select_terms=[fn.Count("*")],
                    groupby_terms=[Field("a"), Field("b")],
                    from_term="customers",
                    limit_term=100,
                )
            ),
        )

    def test_select_count_star(self):
        self.assertEqual(
            VizSpec(
                y_axises=[YAxis(name="*", aggregation="COUNT")],
                tables=["customers"],
            ),
            generate_viz_spec(
                SqlTree(select_terms=[fn.Count("*")], from_term="customers")
            ),
        )

    def test_select_count_star_alias(self):
        self.assertEqual(
            VizSpec(
                y_axises=[YAxis(name="*", aggregation="COUNT", alias="num_rows")],
                tables=["customers"],
            ),
            generate_viz_spec(
                SqlTree(
                    select_terms=[fn.Count("*").as_("num_rows")], from_term="customers"
                )
            ),
        )

    def test_select_count_asdf(self):
        self.assertEqual(
            VizSpec(
                y_axises=[YAxis(name="asdf", aggregation="COUNT")],
                tables=["customers"],
            ),
            generate_viz_spec(
                SqlTree(select_terms=[fn.Count("asdf")], from_term="customers"),
            ),
        )

    def test_select_avg_star(self):
        self.assertEqual(
            VizSpec(
                y_axises=[YAxis(name="customerVal", aggregation="AVG")],
                tables=["customers"],
            ),
            generate_viz_spec(
                SqlTree(
                    select_terms=[fn.AggregateFunction("AVG", Field("customerVal"))],
                    from_term="customers",
                ),
            ),
        )

    def test_select_min(self):
        self.assertEqual(
            VizSpec(
                y_axises=[YAxis(name="customerVal", aggregation="MIN")],
                tables=["customers"],
            ),
            generate_viz_spec(
                SqlTree(
                    select_terms=[fn.AggregateFunction("MIN", Field("customerVal"))],
                    from_term="customers",
                ),
            ),
        )

    def test_select_count_avg(self):
        self.assertEqual(
            VizSpec(
                y_axises=[
                    YAxis(name="*", aggregation="COUNT"),
                    YAxis(name="customerVal", aggregation="AVG"),
                ],
                tables=["customers"],
            ),
            generate_viz_spec(
                SqlTree(
                    select_terms=[
                        fn.Count("*"),
                        fn.AggregateFunction("AVG", Field("customerVal")),
                    ],
                    from_term="customers",
                ),
            ),
        )

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
                            "strftime", "%Y-%m-%d %H:%M:%S", Field("started_at")
                        )
                    ],
                    groupby_terms=[
                        fn.Function(
                            "strftime", "%Y-%m-%d %H:%M:%S", Field("started_at")
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
                            "strftime", "%Y-%m-%d %H:%M:%S", Field("started_at")
                        )
                    ],
                    groupby_terms=[
                        Field("a"),
                        fn.Function(
                            "strftime", "%Y-%m-%d %H:%M:%S", Field("started_at")
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
                            "strftime", "%Y-%m-%d %H:%M:%S", Field("started_at")
                        )
                    ],
                    groupby_terms=[
                        fn.Function(
                            "strftime", "%Y-%m-%d %H:%M:%S", Field("started_at")
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
                            "strftime", "%Y-%m-%d %H:%M:%S", Field("started_at")
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
                        fn.Function("strftime", "%Y-%m-%d %H:%M", Field("started_at"))
                    ],
                    groupby_terms=[
                        fn.Function("strftime", "%Y-%m-%d %H:%M", Field("started_at"))
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
                        fn.Function("strftime", "%Y-%m-%d %H", Field("started_at"))
                    ],
                    groupby_terms=[
                        fn.Function("strftime", "%Y-%m-%d %H", Field("started_at"))
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
                        fn.Function("strftime", "%Y-%m-%d", Field("started_at"))
                    ],
                    groupby_terms=[
                        fn.Function("strftime", "%Y-%m-%d", Field("started_at"))
                    ],
                    from_term="customers",
                ),
            ),
        )

    def test_select_datetime_x_weeks(self):
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
                        fn.Function(
                            "strftime",
                            "%Y-%m-%d",
                            Field("started_at"),
                            "weekday 0",
                            "-6 days",
                        )
                    ],
                    groupby_terms=[
                        fn.Function(
                            "strftime",
                            "%Y-%m-%d",
                            Field("started_at"),
                            "weekday 0",
                            "-6 days",
                        )
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
                        fn.Function("strftime", "%Y-%m", Field("started_at"))
                    ],
                    groupby_terms=[
                        fn.Function("strftime", "%Y-%m", Field("started_at"))
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
                    select_terms=[fn.Function("strftime", "%Y", Field("started_at"))],
                    groupby_terms=[fn.Function("strftime", "%Y", Field("started_at"))],
                    from_term="customers",
                ),
            ),
        )

    def test_select_group_by_order_by(self):
        self.assertEqual(
            VizSpec(
                x_axis=XAxis(name="a"),
                tables=["customers"],
                sort_by=SortBy(name="a"),
            ),
            generate_viz_spec(
                SqlTree(
                    select_terms=[Field("a")],
                    groupby_terms=[Field("a")],
                    from_term="customers",
                    orderby_term=(Field("a"), "ASC"),
                ),
            ),
        )

    def test_select_group_by_order_by_desc(self):
        self.assertEqual(
            VizSpec(
                x_axis=XAxis(name="a"),
                tables=["customers"],
                sort_by=SortBy(name="a", direction="desc"),
            ),
            generate_viz_spec(
                SqlTree(
                    select_terms=[Field("a")],
                    groupby_terms=[Field("a")],
                    from_term="customers",
                    orderby_term=(Field("a"), "DESC"),
                ),
            ),
        )

    def test_select_order_by_y_axis_aggregation(self):
        self.assertEqual(
            VizSpec(
                x_axis=XAxis(name="a"),
                y_axises=[YAxis(name="b", aggregation="COUNT")],
                tables=["customers"],
                sort_by=SortBy(name="a", direction="desc"),
            ),
            generate_viz_spec(
                SqlTree(
                    select_terms=[Field("a"), fn.Count("b")],
                    groupby_terms=[Field("a")],
                    from_term="customers",
                    orderby_term=(Field("a"), "DESC"),
                ),
            ),
        )

    def test_select_order_by_y_axis_aggregation_star(self):
        self.assertEqual(
            VizSpec(
                x_axis=XAxis(name="a"),
                y_axises=[YAxis(name="*", aggregation="COUNT")],
                tables=["customers"],
                sort_by=SortBy(name="a", direction="desc"),
            ),
            generate_viz_spec(
                SqlTree(
                    select_terms=[Field("a"), fn.Count("*")],
                    groupby_terms=[Field("a")],
                    from_term="customers",
                    orderby_term=(Field("a"), "DESC"),
                ),
            ),
        )

    def test_select_order_by_y_axis_count(self):
        self.assertEqual(
            VizSpec(
                x_axis=XAxis(name="a"),
                y_axises=[YAxis(name="b", aggregation="COUNT")],
                tables=["customers"],
                sort_by=SortBy(name="b", direction="desc"),
            ),
            generate_viz_spec(
                SqlTree(
                    select_terms=[Field("a"), fn.Count("b")],
                    groupby_terms=[Field("a")],
                    from_term="customers",
                    orderby_term=(fn.Count("b"), "DESC"),
                ),
            ),
        )

    def test_select_order_by_y_axis_count_alias(self):
        self.assertEqual(
            VizSpec(
                x_axis=XAxis(name="a"),
                y_axises=[YAxis(name="b", aggregation="COUNT", alias="countb")],
                tables=["customers"],
                sort_by=SortBy(name="b", direction="desc"),
            ),
            generate_viz_spec(
                SqlTree(
                    select_terms=[Field("a"), fn.Count("b").as_("countb")],
                    groupby_terms=[Field("a")],
                    from_term="customers",
                    orderby_term=(Field("countb"), "DESC"),
                ),
            ),
        )

    def test_select_order_by_x_axis_bin(self):
        self.assertEqual(
            VizSpec(
                x_axis=XAxis(
                    name="started_at",
                    binner=Binner(binner_type="datetime", time_unit="year"),
                ),
                tables=["customers"],
                sort_by=SortBy(name="started_at", direction="asc"),
            ),
            generate_viz_spec(
                SqlTree(
                    select_terms=[fn.Function("strftime", "%Y", Field("started_at"))],
                    groupby_terms=[fn.Function("strftime", "%Y", Field("started_at"))],
                    orderby_term=(
                        fn.Function("strftime", "%Y", Field("started_at")),
                        "ASC",
                    ),
                    from_term="customers",
                ),
            ),
        )

    def test_select_order_by_x_axis_bin_alias(self):
        self.assertEqual(
            VizSpec(
                x_axis=XAxis(
                    name="started_at",
                    binner=Binner(binner_type="datetime", time_unit="year"),
                    alias="started_at_year",
                ),
                tables=["customers"],
                sort_by=SortBy(name="started_at", direction="asc"),
            ),
            generate_viz_spec(
                SqlTree(
                    select_terms=[
                        fn.Function("strftime", "%Y", Field("started_at")).as_(
                            "started_at_year"
                        )
                    ],
                    groupby_terms=[Field("started_at_year")],
                    orderby_term=(
                        Field("started_at_year"),
                        "ASC",
                    ),
                    from_term="customers",
                ),
            ),
        )

    def test_select_order_by_breakdown(self):
        self.assertEqual(
            VizSpec(
                x_axis=XAxis(name="a"),
                breakdowns=[Breakdown(name="b")],
                tables=["customers"],
                sort_by=SortBy(name="b"),
            ),
            generate_viz_spec(
                SqlTree(
                    select_terms=[Field("a"), Field("b")],
                    groupby_terms=[Field("a"), Field("b")],
                    orderby_term=[Field("b"), "ASC"],
                    from_term="customers",
                ),
            ),
        )

    def test_select_or_filter(self):
        self.assertEqual(
            VizSpec(
                y_axises=[YAxis(name="*")],
                tables=["customers"],
                filters=[
                    Filter(
                        name="complex_filter",
                        filter_type="complex",
                        expression="department = 'foo' or department = 'bar'",
                    )
                ],
            ),
            generate_viz_spec(
                SqlTree(
                    select_terms=["*"],
                    from_term="customers",
                    where_term=(
                        (Field("department") == "foo") | (Field("department") == "bar")
                    ),
                ),
            ),
        )

    def test_select_x_axis_domain_min(self):
        self.assertEqual(
            VizSpec(
                x_axis=XAxis(name="a", domain=[10, None]),
                tables=["customers"],
            ),
            generate_viz_spec(
                SqlTree(
                    select_terms=[Field("a")],
                    groupby_terms=[Field("a")],
                    from_term="customers",
                    where_term=(Field("a") >= 10),
                ),
            ),
        )

    def test_select_x_axis_domain_max(self):
        self.assertEqual(
            VizSpec(
                x_axis=XAxis(name="a", domain=[None, 10]),
                tables=["customers"],
            ),
            generate_viz_spec(
                SqlTree(
                    select_terms=[Field("a")],
                    groupby_terms=[Field("a")],
                    from_term="customers",
                    where_term=(Field("a") <= 10),
                ),
            ),
        )

    def test_select_x_axis_domain_decimal_max(self):
        self.assertEqual(
            VizSpec(
                x_axis=XAxis(name="a", domain=[None, 20.5]),
                tables=["customers"],
            ),
            generate_viz_spec(
                SqlTree(
                    select_terms=[Field("a")],
                    groupby_terms=[Field("a")],
                    from_term="customers",
                    where_term=(Field("a") <= 20.5),
                ),
            ),
        )

    def test_select_x_axis_domain_string(self):
        self.assertEqual(
            VizSpec(
                x_axis=XAxis(name="a", domain=[None, "bar"]),
                tables=["customers"],
            ),
            generate_viz_spec(
                SqlTree(
                    select_terms=[Field("a")],
                    groupby_terms=[Field("a")],
                    from_term="customers",
                    where_term=(Field("a") <= "bar"),
                ),
            ),
        )

    def test_select_x_axis_domain_between(self):
        self.assertEqual(
            VizSpec(
                x_axis=XAxis(name="a", domain=[10, 20]),
                tables=["customers"],
            ),
            generate_viz_spec(
                SqlTree(
                    select_terms=[Field("a")],
                    groupby_terms=[Field("a")],
                    from_term="customers",
                    where_term=(Field("a")[10:20]),
                ),
            ),
        )

    def test_select_x_axis_domain_between_computed(self):
        self.assertEqual(
            VizSpec(
                x_axis=XAxis(
                    name="started_at",
                    binner=Binner(binner_type="datetime", time_unit="year"),
                    domain=[10, 20],
                ),
                tables=["customers"],
            ),
            generate_viz_spec(
                SqlTree(
                    select_terms=[fn.Function("strftime", "%Y", Field("started_at"))],
                    groupby_terms=[fn.Function("strftime", "%Y", Field("started_at"))],
                    from_term="customers",
                    where_term=(
                        fn.Function("strftime", "%Y", Field("started_at"))[10:20]
                    ),
                ),
            ),
        )

    def test_select_x_axis_domain_between_computed_alias(self):
        self.assertEqual(
            VizSpec(
                x_axis=XAxis(
                    name="started_at",
                    alias="started_at_year",
                    binner=Binner(binner_type="datetime", time_unit="year"),
                    domain=[10, 20],
                ),
                tables=["customers"],
            ),
            generate_viz_spec(
                SqlTree(
                    select_terms=[
                        fn.Function("strftime", "%Y", Field("started_at")).as_(
                            "started_at_year"
                        )
                    ],
                    groupby_terms=[Field("started_at_year")],
                    from_term="customers",
                    where_term=(Field("started_at_year")[10:20]),
                ),
            ),
        )

    def test_select_x_axis_domain_between_computed_alias_irderby(self):
        self.assertEqual(
            VizSpec(
                x_axis=XAxis(
                    name="started_at",
                    alias="started_at_year",
                    binner=Binner(binner_type="datetime", time_unit="year"),
                    domain=[10, 20],
                ),
                sort_by=SortBy(name="started_at", direction="desc"),
                tables=["customers"],
            ),
            generate_viz_spec(
                SqlTree(
                    select_terms=[
                        fn.Function("strftime", "%Y", Field("started_at")).as_(
                            "started_at_year"
                        )
                    ],
                    groupby_terms=[Field("started_at_year")],
                    from_term="customers",
                    where_term=(Field("started_at_year")[10:20]),
                    orderby_term=(Field("started_at_year"), "DESC"),
                ),
            ),
        )

    def test_select_filter_simple_eq(self):
        self.assertEqual(
            VizSpec(
                y_axises=[YAxis(name="*")],
                tables=["customers"],
                filters=[Filter(name="foo", filter_type="comparison", values=["bar"])],
            ),
            generate_viz_spec(
                SqlTree(
                    select_terms=["*"],
                    from_term="customers",
                    where_term=(Field("foo") == "bar"),
                ),
            ),
        )

    def test_select_filter_simple_not_eq(self):
        self.assertEqual(
            VizSpec(
                y_axises=[YAxis(name="*")],
                tables=["customers"],
                filters=[
                    Filter(
                        name="foo",
                        filter_type="comparison",
                        values=["bar"],
                        negate=True,
                    )
                ],
            ),
            generate_viz_spec(
                SqlTree(
                    select_terms=["*"],
                    from_term="customers",
                    where_term=(Field("foo") != "bar"),
                ),
            ),
        )

    def test_select_filter_simple_not_eq_2(self):
        self.assertEqual(
            VizSpec(
                y_axises=[YAxis(name="*")],
                tables=["customers"],
                filters=[
                    Filter(
                        name="foo",
                        filter_type="comparison",
                        values=["bar"],
                        negate=True,
                    )
                ],
            ),
            generate_viz_spec(
                SqlTree(
                    select_terms=["*"],
                    from_term="customers",
                    where_term=(Field("foo") == "bar").negate(),
                ),
            ),
        )

    def test_select_filter_simple_eq_flipped(self):
        self.assertEqual(
            VizSpec(
                y_axises=[YAxis(name="*")],
                tables=["customers"],
                filters=[Filter(name="foo", filter_type="comparison", values=["bar"])],
            ),
            generate_viz_spec(
                SqlTree(
                    select_terms=["*"],
                    from_term="customers",
                    where_term=("bar" == Field("foo")),
                ),
            ),
        )

    def test_select_filter_simple_in(self):
        self.assertEqual(
            VizSpec(
                y_axises=[YAxis(name="*")],
                tables=["customers"],
                filters=[
                    Filter(name="foo", filter_type="comparison", values=["bar", "bar2"])
                ],
            ),
            generate_viz_spec(
                SqlTree(
                    select_terms=["*"],
                    from_term="customers",
                    where_term=(Field("foo").isin(["bar", "bar2"])),
                ),
            ),
        )

    def test_select_filter_simple_gt(self):
        self.assertEqual(
            VizSpec(
                y_axises=[YAxis(name="*")],
                tables=["customers"],
                filters=[Filter(name="foo", filter_type="numeric", domain=[10, None])],
            ),
            generate_viz_spec(
                SqlTree(
                    select_terms=["*"],
                    from_term="customers",
                    where_term=(Field("foo") > 10),
                ),
            ),
        )

    def test_select_filter_simple_not_gt(self):
        self.assertEqual(
            VizSpec(
                y_axises=[YAxis(name="*")],
                tables=["customers"],
                filters=[
                    Filter(
                        name="foo",
                        filter_type="numeric",
                        domain=[10, None],
                        negate=True,
                    )
                ],
            ),
            generate_viz_spec(
                SqlTree(
                    select_terms=["*"],
                    from_term="customers",
                    where_term=(Field("foo") > 10).negate(),
                ),
            ),
        )

    def test_select_filter_simple_like(self):
        self.assertEqual(
            VizSpec(
                y_axises=[YAxis(name="*")],
                tables=["customers"],
                filters=[Filter(name="foo", filter_type="like", values=["%MC%"])],
            ),
            generate_viz_spec(
                SqlTree(
                    select_terms=["*"],
                    from_term="customers",
                    where_term=(Field("foo").like("%MC%")),
                ),
            ),
        )

    def test_select_filter_simple_not_like(self):
        self.assertEqual(
            VizSpec(
                y_axises=[YAxis(name="*")],
                tables=["customers"],
                filters=[
                    Filter(name="foo", filter_type="like", values=["%MC%"], negate=True)
                ],
            ),
            generate_viz_spec(
                SqlTree(
                    select_terms=["*"],
                    from_term="customers",
                    where_term=(Field("foo").like("%MC%")).negate(),
                ),
            ),
        )

    def test_select_filter_simple_lt(self):
        self.assertEqual(
            VizSpec(
                y_axises=[YAxis(name="*")],
                tables=["customers"],
                filters=[Filter(name="foo", filter_type="numeric", domain=[None, 10])],
            ),
            generate_viz_spec(
                SqlTree(
                    select_terms=["*"],
                    from_term="customers",
                    where_term=(Field("foo") < 10),
                ),
            ),
        )

    def test_select_filter_simple_lt_not(self):
        self.assertEqual(
            VizSpec(
                y_axises=[YAxis(name="*")],
                tables=["customers"],
                filters=[
                    Filter(
                        name="foo",
                        filter_type="numeric",
                        domain=[None, 10],
                        negate=True,
                    )
                ],
            ),
            generate_viz_spec(
                SqlTree(
                    select_terms=["*"],
                    from_term="customers",
                    where_term=(Field("foo") < 10).negate(),
                ),
            ),
        )

    def test_select_filter_simple_gt_flipped(self):
        self.assertEqual(
            VizSpec(
                y_axises=[YAxis(name="*")],
                tables=["customers"],
                filters=[Filter(name="foo", filter_type="numeric", domain=[None, 10])],
            ),
            generate_viz_spec(
                SqlTree(
                    select_terms=["*"],
                    from_term="customers",
                    where_term=(10 > Field("foo")),
                ),
            ),
        )

    def test_select_filter_simple_lt_flipped(self):
        self.assertEqual(
            VizSpec(
                y_axises=[YAxis(name="*")],
                tables=["customers"],
                filters=[Filter(name="foo", filter_type="numeric", domain=[10, None])],
            ),
            generate_viz_spec(
                SqlTree(
                    select_terms=["*"],
                    from_term="customers",
                    where_term=(10 < Field("foo")),
                ),
            ),
        )

    def test_select_filter_simple_between(self):
        self.assertEqual(
            VizSpec(
                y_axises=[YAxis(name="*")],
                tables=["customers"],
                filters=[Filter(name="foo", filter_type="numeric", domain=[10, 20])],
            ),
            generate_viz_spec(
                SqlTree(
                    select_terms=["*"],
                    from_term="customers",
                    where_term=(Field("foo")[10:20]),
                ),
            ),
        )

    def test_select_filter_simple_between_negate(self):
        self.assertEqual(
            VizSpec(
                y_axises=[YAxis(name="*")],
                tables=["customers"],
                filters=[
                    Filter(
                        name="foo", filter_type="numeric", domain=[10, 20], negate=True
                    )
                ],
            ),
            generate_viz_spec(
                SqlTree(
                    select_terms=["*"],
                    from_term="customers",
                    where_term=(Field("foo")[10:20]).negate(),
                ),
            ),
        )

    def test_select_filter_multiple_negate_one(self):
        self.assertEqual(
            VizSpec(
                y_axises=[YAxis(name="*")],
                tables=["customers"],
                filters=[
                    Filter(name="foo", filter_type="numeric", domain=[10, 20]),
                    Filter(
                        name="bar",
                        filter_type="comparison",
                        values=["asdf"],
                        negate=True,
                    ),
                ],
            ),
            generate_viz_spec(
                SqlTree(
                    select_terms=["*"],
                    from_term="customers",
                    where_term=(
                        Field("foo")[10:20] & (Field("bar") == "asdf").negate()
                    ),
                ),
            ),
        )

    def test_select_filter_multiple_x_axis_domain(self):
        self.assertEqual(
            VizSpec(
                x_axis=XAxis(name="a", domain=[10, 20]),
                y_axises=[YAxis(name="*", aggregation="COUNT")],
                tables=["customers"],
                filters=[
                    Filter(name="foo", filter_type="numeric", domain=[10, 20]),
                    Filter(name="bar", filter_type="comparison", values=["asdf"]),
                ],
            ),
            generate_viz_spec(
                SqlTree(
                    select_terms=[Field("a"), fn.Count("*")],
                    from_term="customers",
                    groupby_terms=[Field("a")],
                    where_term=(
                        Field("a")[10:20]
                        & Field("foo")[10:20]
                        & (Field("bar") == "asdf")
                    ),
                ),
            ),
        )

    def test_select_groupby_func(self):
        self.assertEqual(
            VizSpec(
                x_axis=XAxis(
                    name="started_at",
                    binner=Binner(binner_type="datetime", time_unit="day"),
                ),
                y_axises=[YAxis(name="*", aggregation="COUNT", alias="num_trips")],
                tables=["JC_202307_citibike_tripdata"],
                limit=500,
            ),
            generate_viz_spec(
                SqlTree(
                    select_terms=[
                        fn.Function("strftime", "%Y-%m-%d", Field("started_at")),
                        fn.Count("*").as_("num_trips"),
                    ],
                    from_term="JC_202307_citibike_tripdata",
                    groupby_terms=[
                        fn.Function("strftime", "%Y-%m-%d", Field("started_at"))
                    ],
                    limit_term=500,
                ),
            ),
        )

    def test_top_market_segments_subquery(self):
        self.assertEqual(
            VizSpec(
                y_axises=[
                    YAxis(
                        name="COUNT(*) * 100 / (select COUNT(*) from ORDERS)",
                        alias="percentage_returned",
                        unparsed=True,
                    )
                ],
                tables=["ORDERS"],
                filters=[
                    Filter(
                        name="O_ORDERSTATUS",
                        filter_type="comparison",
                        values=["RETURNED"],
                    ),
                ],
                limit=500,
            ),
            generate_viz_spec(
                SqlTree(
                    select_terms=[
                        UnparsedField(
                            "COUNT(*) * 100 / (select COUNT(*) from ORDERS)"
                        ).as_("percentage_returned")
                    ],
                    from_term="ORDERS",
                    where_term=(Field("O_ORDERSTATUS") == "RETURNED"),
                    limit_term=500,
                ),
            ),
        )

    def test_gender_percentages(self):
        self.assertEqual(
            VizSpec(
                y_axises=[
                    YAxis(
                        name="*",
                        aggregation="COUNT",
                        alias="count",
                    ),
                    YAxis(
                        name="(COUNT(*) * 100 / (select COUNT(*) from data))",
                        alias="Percentage",
                        unparsed=True,
                    ),
                ],
                tables=["data"],
                x_axis=XAxis(name="Gender"),
                limit=500,
            ),
            generate_viz_spec(
                SqlTree(
                    select_terms=[
                        fn.Count("*").as_("count"),
                        UnparsedField(
                            "(COUNT(*) * 100 / (select COUNT(*) from data))"
                        ).as_("Percentage"),
                    ],
                    from_term="data",
                    groupby_terms=[Field("Gender")],
                    limit_term=500,
                ),
            ),
        )

    def test_average_trip_duration(self):
        self.assertEqual(
            VizSpec(
                y_axises=[
                    YAxis(
                        name="(julianday(ended_at) - julianday(started_at)) * '24'",
                        aggregation="AVG",
                        unparsed=True,
                    ),
                ],
                tables=["citibike_partial_demo"],
            ),
            generate_viz_spec(
                SqlTree(
                    select_terms=[
                        fn.AggregateFunction(
                            "AVG",
                            ArithmeticExpression(
                                operator=Arithmetic.mul,
                                left=ArithmeticExpression(
                                    operator=Arithmetic.sub,
                                    left=fn.Function("julianday", Field("ended_at")),
                                    right=fn.Function("julianday", Field("started_at")),
                                ),
                                right=ValueWrapper("24"),
                            ),
                        )
                    ],
                    from_term="citibike_partial_demo",
                ),
            ),
        )

    def test_unparsed_x(self):
        self.assertEqual(
            VizSpec(
                x_axis=XAxis(name="YEAR(start_time)", alias="year", unparsed=True),
                tables=["citibike_partial_demo"],
            ),
            generate_viz_spec(
                SqlTree(
                    select_terms=[UnparsedField("YEAR(start_time)").as_("year")],
                    groupby_terms=[Field("year")],
                    from_term="citibike_partial_demo",
                ),
            ),
        )

    def test_select_join_on(self):
        self.assertEqual(
            VizSpec(
                y_axises=[YAxis(name="*")],
                tables=["customer", "orders"],
            ),
            generate_viz_spec(
                SqlTree(
                    select_terms=["*"],
                    from_term="customer",
                    joinon_terms=[
                        (
                            Table("orders"),
                            Field("customer.id") == Field("orders.id"),
                        )
                    ],
                )
            ),
        )

    def test_select_join_multiple(self):
        self.assertEqual(
            VizSpec(
                y_axises=[YAxis(name="*")],
                tables=["customer", "orders", "lineitems"],
            ),
            generate_viz_spec(
                SqlTree(
                    select_terms=["*"],
                    from_term="customer",
                    joinon_terms=[
                        (
                            Table("orders"),
                            Field("customer.id") == Field("orders.id"),
                        ),
                        (
                            Table("lineitems"),
                            Field("orders.id") == Field("lineitems.id"),
                        ),
                    ],
                )
            ),
        )

    def test_select_join_multiple_alias(self):
        self.assertEqual(
            VizSpec(
                y_axises=[YAxis(name="*")],
                tables=["customer", "orders", "lineitems"],
            ),
            generate_viz_spec(
                SqlTree(
                    select_terms=["*"],
                    from_term=Table("customer").as_("c"),
                    joinon_terms=[
                        (
                            Table("orders").as_("o"),
                            Field("c.id") == Field("o.id"),
                        ),
                        (
                            Table("lineitems").as_("l"),
                            Field("o.id") == Field("l.id"),
                        ),
                    ],
                )
            ),
        )
