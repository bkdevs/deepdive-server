from pypika import Table
from pypika import functions as fn
from pypika.terms import Field, LiteralValue

from deepdive.schema import (
    Binner,
    Breakdown,
    DatabaseSchema,
    Filter,
    SortBy,
    VizSpec,
    XAxis,
    YAxis,
    ForeignKey,
)
from deepdive.sql.parser.sql_tree import SqlTree
from deepdive.test.sql.sql_test_case import SqlTestCase, compile_viz_spec
from deepdive.viz.compiler.sqlite_compiler import SqliteCompiler


class TestCompiler(SqlTestCase):
    """
    Tests that we can compile a viz spec into a SqlTree
    """

    def test_simple_select(self):
        self.assertTreeEquals(
            SqlTree(select_terms=[Field("a")], from_term="customers"),
            compile_viz_spec(
                VizSpec(
                    y_axises=[YAxis(name="a")],
                    tables=["customers"],
                )
            ),
        )

    def test_simple_select_two_columns(self):
        self.assertTreeEquals(
            SqlTree(select_terms=[Field("a"), Field("b")], from_term="customers"),
            compile_viz_spec(
                VizSpec(
                    y_axises=[YAxis(name="a"), YAxis(name="b")],
                    tables=["customers"],
                )
            ),
        )

    def test_simple_select_star(self):
        self.assertTreeEquals(
            SqlTree(select_terms=["*"], from_term="customers"),
            compile_viz_spec(
                VizSpec(
                    y_axises=[YAxis(name="*")],
                    tables=["customers"],
                )
            ),
        )

    def test_select_limit(self):
        self.assertTreeEquals(
            SqlTree(select_terms=[Field("a")], from_term="customers", limit_term=500),
            compile_viz_spec(
                VizSpec(
                    y_axises=[YAxis(name="a")],
                    tables=["customers"],
                    limit=500,
                )
            ),
        )

    def test_select_group_by(self):
        self.assertTreeEquals(
            SqlTree(
                select_terms=[Field("a")],
                groupby_terms=[Field("a")],
                from_term="customers",
            ),
            compile_viz_spec(
                VizSpec(
                    breakdowns=[Breakdown(name="a")],
                    tables=["customers"],
                )
            ),
        )

    def test_select_group_by_alias(self):
        self.assertTreeEquals(
            SqlTree(
                select_terms=[Field("a").as_("b")],
                groupby_terms=[Field("b")],
                from_term="customers",
            ),
            compile_viz_spec(
                VizSpec(
                    breakdowns=[Breakdown(name="a", alias="b")],
                    tables=["customers"],
                )
            ),
        )

    def test_select_group_by_xaxis(self):
        self.assertTreeEquals(
            SqlTree(
                select_terms=[Field("a")],
                groupby_terms=[Field("a")],
                from_term="customers",
            ),
            compile_viz_spec(
                VizSpec(
                    x_axis=XAxis(name="a"),
                    tables=["customers"],
                )
            ),
        )

    def test_select_group_by_xaxis_alias(self):
        self.assertTreeEquals(
            SqlTree(
                select_terms=[Field("a").as_("b")],
                groupby_terms=[Field("b")],
                from_term="customers",
            ),
            compile_viz_spec(
                VizSpec(
                    x_axis=XAxis(name="a", alias="b"),
                    tables=["customers"],
                )
            ),
        )

    def test_select_group_by_limit(self):
        self.assertTreeEquals(
            SqlTree(
                select_terms=[Field("a")],
                groupby_terms=[Field("a")],
                from_term="customers",
                limit_term=100,
            ),
            compile_viz_spec(
                VizSpec(
                    x_axis=XAxis(name="a"),
                    tables=["customers"],
                    limit=100,
                )
            ),
        )

    def test_select_few_group_by_limit(self):
        self.assertTreeEquals(
            SqlTree(
                select_terms=[Field("a"), fn.Count("b")],
                groupby_terms=[Field("a")],
                from_term="customers",
                limit_term=100,
            ),
            compile_viz_spec(
                VizSpec(
                    y_axises=[YAxis(name="b", aggregation="COUNT")],
                    breakdowns=[Breakdown(name="a")],
                    tables=["customers"],
                    limit=100,
                )
            ),
        )

    def test_select_few_group_by_few_limit(self):
        self.assertTreeEquals(
            SqlTree(
                select_terms=[Field("a"), Field("b")],
                groupby_terms=[Field("a"), Field("b")],
                from_term="customers",
                limit_term=100,
            ),
            compile_viz_spec(
                VizSpec(
                    breakdowns=[Breakdown(name="a"), Breakdown(name="b")],
                    tables=["customers"],
                    limit=100,
                )
            ),
        )

    def test_select_few_group_by_few_limit_2(self):
        self.assertTreeEquals(
            SqlTree(
                select_terms=[Field("a"), Field("b")],
                groupby_terms=[Field("a"), Field("b")],
                from_term="customers",
                limit_term=100,
            ),
            compile_viz_spec(
                VizSpec(
                    x_axis=XAxis(name="a"),
                    breakdowns=[Breakdown(name="b")],
                    tables=["customers"],
                    limit=100,
                )
            ),
        )

    def test_select_start_groupby_limit(self):
        self.assertTreeEquals(
            SqlTree(
                select_terms=[Field("a"), Field("b"), fn.Count("*")],
                groupby_terms=[Field("a"), Field("b")],
                from_term="customers",
                limit_term=100,
            ),
            compile_viz_spec(
                VizSpec(
                    y_axises=[YAxis(name="*", aggregation="COUNT")],
                    breakdowns=[Breakdown(name="a"), Breakdown(name="b")],
                    tables=["customers"],
                    limit=100,
                )
            ),
        )

    def test_select_count_star(self):
        query = "select COUNT(*) from customers"
        self.assertTreeEquals(
            SqlTree(select_terms=[fn.Count("*")], from_term="customers"),
            compile_viz_spec(
                VizSpec(
                    y_axises=[YAxis(name="*", aggregation="COUNT")],
                    tables=["customers"],
                )
            ),
        )

    def test_select_count_asdf(self):
        self.assertTreeEquals(
            SqlTree(select_terms=[fn.Count("asdf")], from_term="customers"),
            compile_viz_spec(
                VizSpec(
                    y_axises=[YAxis(name="asdf", aggregation="COUNT")],
                    tables=["customers"],
                )
            ),
        )

    def test_select_avg_star(self):
        self.assertTreeEquals(
            SqlTree(
                select_terms=[fn.AggregateFunction("AVG", Field("customerVal"))],
                from_term="customers",
            ),
            compile_viz_spec(
                VizSpec(
                    y_axises=[YAxis(name="customerVal", aggregation="AVG")],
                    tables=["customers"],
                )
            ),
        )

    def test_select_min(self):
        self.assertTreeEquals(
            SqlTree(
                select_terms=[fn.AggregateFunction("MIN", Field("customerVal"))],
                from_term="customers",
            ),
            compile_viz_spec(
                VizSpec(
                    y_axises=[YAxis(name="customerVal", aggregation="MIN")],
                    tables=["customers"],
                )
            ),
        )

    def test_select_avg_max(self):
        self.assertTreeEquals(
            SqlTree(
                select_terms=[
                    fn.AggregateFunction("AVG", Field("customerVal")),
                    fn.AggregateFunction("MAX", Field("customerVal")),
                ],
                from_term="customers",
            ),
            compile_viz_spec(
                VizSpec(
                    y_axises=[
                        YAxis(name="customerVal", aggregation="AVG"),
                        YAxis(name="customerVal", aggregation="MAX"),
                    ],
                    tables=["customers"],
                )
            ),
        )

    def test_select_count_avg(self):
        query = "select COUNT(*), AVG(customerVal) from customers"
        self.assertTreeEquals(
            SqlTree(
                select_terms=[
                    fn.Count("*"),
                    fn.AggregateFunction("AVG", Field("customerVal")),
                ],
                from_term="customers",
            ),
            compile_viz_spec(
                VizSpec(
                    y_axises=[
                        YAxis(name="*", aggregation="COUNT"),
                        YAxis(name="customerVal", aggregation="AVG"),
                    ],
                    tables=["customers"],
                )
            ),
        )

    def test_select_datetime_x(self):
        self.assertTreeEquals(
            SqlTree(
                select_terms=[
                    fn.Function("strftime", "%Y-%m-%d %H:%M:%S", Field("started_at"))
                ],
                groupby_terms=[
                    fn.Function("strftime", "%Y-%m-%d %H:%M:%S", Field("started_at"))
                ],
                from_term="customers",
            ),
            compile_viz_spec(
                VizSpec(
                    x_axis=XAxis(
                        name="started_at",
                        binner=Binner(binner_type="datetime", time_unit="second"),
                    ),
                    tables=["customers"],
                )
            ),
        )

    def test_select_datetime_x_alias(self):
        self.assertTreeEquals(
            SqlTree(
                select_terms=[
                    fn.Function(
                        "strftime", "%Y-%m-%d %H:%M:%S", Field("started_at")
                    ).as_("started_at_seconds")
                ],
                groupby_terms=[Field("started_at_seconds")],
                from_term="customers",
            ),
            compile_viz_spec(
                VizSpec(
                    x_axis=XAxis(
                        name="started_at",
                        binner=Binner(binner_type="datetime", time_unit="second"),
                        alias="started_at_seconds",
                    ),
                    tables=["customers"],
                )
            ),
        )

    def test_select_datetime_x_minutes(self):
        self.assertTreeEquals(
            SqlTree(
                select_terms=[
                    fn.Function("strftime", "%Y-%m-%d %H:%M", Field("started_at"))
                ],
                groupby_terms=[
                    fn.Function("strftime", "%Y-%m-%d %H:%M", Field("started_at"))
                ],
                from_term="customers",
            ),
            compile_viz_spec(
                VizSpec(
                    x_axis=XAxis(
                        name="started_at",
                        binner=Binner(binner_type="datetime", time_unit="minute"),
                    ),
                    tables=["customers"],
                )
            ),
        )

    def test_select_datetime_x_hours(self):
        self.assertTreeEquals(
            SqlTree(
                select_terms=[
                    fn.Function("strftime", "%Y-%m-%d %H", Field("started_at"))
                ],
                groupby_terms=[
                    fn.Function("strftime", "%Y-%m-%d %H", Field("started_at"))
                ],
                from_term="customers",
            ),
            compile_viz_spec(
                VizSpec(
                    x_axis=XAxis(
                        name="started_at",
                        binner=Binner(binner_type="datetime", time_unit="hour"),
                    ),
                    tables=["customers"],
                )
            ),
        )

    def test_select_datetime_x_days(self):
        self.assertTreeEquals(
            SqlTree(
                select_terms=[fn.Function("strftime", "%Y-%m-%d", Field("started_at"))],
                groupby_terms=[
                    fn.Function("strftime", "%Y-%m-%d", Field("started_at"))
                ],
                from_term="customers",
            ),
            compile_viz_spec(
                VizSpec(
                    x_axis=XAxis(
                        name="started_at",
                        binner=Binner(binner_type="datetime", time_unit="day"),
                    ),
                    tables=["customers"],
                )
            ),
        )

    def test_select_datetime_x_weeks(self):
        self.assertTreeEquals(
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
            compile_viz_spec(
                VizSpec(
                    x_axis=XAxis(
                        name="started_at",
                        binner=Binner(binner_type="datetime", time_unit="week"),
                    ),
                    tables=["customers"],
                )
            ),
        )

    def test_select_datetime_x_months(self):
        self.assertTreeEquals(
            SqlTree(
                select_terms=[fn.Function("strftime", "%Y-%m", Field("started_at"))],
                groupby_terms=[fn.Function("strftime", "%Y-%m", Field("started_at"))],
                from_term="customers",
            ),
            compile_viz_spec(
                VizSpec(
                    x_axis=XAxis(
                        name="started_at",
                        binner=Binner(binner_type="datetime", time_unit="month"),
                    ),
                    tables=["customers"],
                )
            ),
        )

    def test_select_datetime_x_years(self):
        self.assertTreeEquals(
            SqlTree(
                select_terms=[fn.Function("strftime", "%Y", Field("started_at"))],
                groupby_terms=[fn.Function("strftime", "%Y", Field("started_at"))],
                from_term="customers",
            ),
            compile_viz_spec(
                VizSpec(
                    x_axis=XAxis(
                        name="started_at",
                        binner=Binner(binner_type="datetime", time_unit="year"),
                    ),
                    tables=["customers"],
                )
            ),
        )

    def test_select_group_by_order_by(self):
        self.assertTreeEquals(
            SqlTree(
                select_terms=[Field("a")],
                groupby_terms=[Field("a")],
                from_term="customers",
                orderby_term=(Field("a"), "ASC"),
            ),
            compile_viz_spec(
                VizSpec(
                    x_axis=XAxis(name="a"),
                    tables=["customers"],
                    sort_by=SortBy(name="a"),
                )
            ),
        )

    def test_select_group_by_order_by_desc(self):
        self.assertTreeEquals(
            SqlTree(
                select_terms=[Field("a")],
                groupby_terms=[Field("a")],
                from_term="customers",
                orderby_term=(Field("a"), "DESC"),
            ),
            compile_viz_spec(
                VizSpec(
                    x_axis=XAxis(name="a"),
                    tables=["customers"],
                    sort_by=SortBy(name="a", direction="desc"),
                )
            ),
        )

    def test_select_order_by_y_axis_aggregation(self):
        self.assertTreeEquals(
            SqlTree(
                select_terms=[Field("a"), fn.Count("b")],
                groupby_terms=[Field("a")],
                from_term="customers",
                orderby_term=(Field("a"), "DESC"),
            ),
            compile_viz_spec(
                VizSpec(
                    x_axis=XAxis(name="a"),
                    y_axises=[YAxis(name="b", aggregation="COUNT")],
                    tables=["customers"],
                    sort_by=SortBy(name="a", direction="desc"),
                )
            ),
        )

    def test_select_order_by_y_axis_aggregation_star(self):
        self.assertTreeEquals(
            SqlTree(
                select_terms=[Field("a"), fn.Count("*")],
                groupby_terms=[Field("a")],
                from_term="customers",
                orderby_term=(Field("a"), "DESC"),
            ),
            compile_viz_spec(
                VizSpec(
                    x_axis=XAxis(name="a"),
                    y_axises=[YAxis(name="*", aggregation="COUNT")],
                    tables=["customers"],
                    sort_by=SortBy(name="a", direction="desc"),
                )
            ),
        )

    def test_select_order_by_y_axis_count(self):
        self.assertTreeEquals(
            SqlTree(
                select_terms=[Field("a"), fn.Count("b")],
                groupby_terms=[Field("a")],
                from_term="customers",
                orderby_term=(fn.Count("b"), "DESC"),
            ),
            compile_viz_spec(
                VizSpec(
                    x_axis=XAxis(name="a"),
                    y_axises=[YAxis(name="b", aggregation="COUNT")],
                    tables=["customers"],
                    sort_by=SortBy(name="b", direction="desc"),
                )
            ),
        )

    def test_select_order_by_y_axis_avg_max(self):
        self.assertTreeEquals(
            SqlTree(
                select_terms=[
                    fn.AggregateFunction("AVG", Field("a")),
                    fn.AggregateFunction("MAX", Field("a")),
                ],
                from_term="customers",
                orderby_term=(fn.AggregateFunction("MAX", Field("a")), "DESC"),
            ),
            compile_viz_spec(
                VizSpec(
                    y_axises=[
                        YAxis(name="a", aggregation="AVG"),
                        YAxis(name="a", aggregation="MAX"),
                    ],
                    tables=["customers"],
                    sort_by=SortBy(
                        name="a", direction="desc"
                    ),  # ambiguous which aggregation to use, we select the last
                    # not sure if this is the right behavior,
                    # maybe the better thing to do on the frontend is just to expose all the terms available?
                    # that seems more germane of a UI... yeah i guess?
                    # clicking sort by AVG(val) seems pretty intuitive
                    # which means what, we render the term string values directly in column?
                )
            ),
        )

    def test_select_order_by_x_axis_bin(self):
        self.assertTreeEquals(
            SqlTree(
                select_terms=[fn.Function("strftime", "%Y", Field("started_at"))],
                groupby_terms=[fn.Function("strftime", "%Y", Field("started_at"))],
                orderby_term=(
                    fn.Function("strftime", "%Y", Field("started_at")),
                    "ASC",
                ),
                from_term="customers",
            ),
            compile_viz_spec(
                VizSpec(
                    x_axis=XAxis(
                        name="started_at",
                        binner=Binner(binner_type="datetime", time_unit="year"),
                    ),
                    tables=["customers"],
                    sort_by=SortBy(name="started_at", direction="asc"),
                )
            ),
        )

    def test_select_order_by_breakdown(self):
        self.assertTreeEquals(
            SqlTree(
                select_terms=[Field("a")],
                groupby_terms=[Field("a")],
                orderby_term=[Field("a"), "ASC"],
                from_term="customers",
            ),
            compile_viz_spec(
                VizSpec(
                    breakdowns=[Breakdown(name="a")],
                    tables=["customers"],
                    sort_by=SortBy(name="a"),
                )
            ),
        )

    def test_select_x_axis_domain_min(self):
        self.assertTreeEquals(
            SqlTree(
                select_terms=[Field("a")],
                groupby_terms=[Field("a")],
                from_term="customers",
                where_term=(Field("a") >= 10),
            ),
            compile_viz_spec(
                VizSpec(
                    x_axis=XAxis(name="a", domain=[10, None]),
                    tables=["customers"],
                )
            ),
        )

    def test_select_x_axis_domain_max(self):
        self.assertTreeEquals(
            SqlTree(
                select_terms=[Field("a")],
                groupby_terms=[Field("a")],
                from_term="customers",
                where_term=(Field("a") <= 10),
            ),
            compile_viz_spec(
                VizSpec(
                    x_axis=XAxis(name="a", domain=[None, 10]),
                    tables=["customers"],
                )
            ),
        )

    def test_select_x_axis_domain_decimal_max(self):
        self.assertTreeEquals(
            SqlTree(
                select_terms=[Field("a")],
                groupby_terms=[Field("a")],
                from_term="customers",
                where_term=(Field("a") <= 20.5),
            ),
            compile_viz_spec(
                VizSpec(
                    x_axis=XAxis(name="a", domain=[None, 20.5]),
                    tables=["customers"],
                )
            ),
        )

    def test_select_x_axis_domain_string(self):
        self.assertTreeEquals(
            SqlTree(
                select_terms=[Field("a")],
                groupby_terms=[Field("a")],
                from_term="customers",
                where_term=(Field("a") <= "bar"),
            ),
            compile_viz_spec(
                VizSpec(
                    x_axis=XAxis(name="a", domain=[None, "bar"]),
                    tables=["customers"],
                )
            ),
        )

    def test_select_x_axis_domain_between(self):
        self.assertTreeEquals(
            SqlTree(
                select_terms=[Field("a")],
                groupby_terms=[Field("a")],
                from_term="customers",
                where_term=(Field("a")[10:20]),
            ),
            compile_viz_spec(
                VizSpec(
                    x_axis=XAxis(name="a", domain=[10, 20]),
                    tables=["customers"],
                )
            ),
        )

    def test_select_x_axis_domain_between_computed(self):
        self.assertTreeEquals(
            SqlTree(
                select_terms=[fn.Function("strftime", "%Y", Field("started_at"))],
                groupby_terms=[fn.Function("strftime", "%Y", Field("started_at"))],
                from_term="customers",
                where_term=(fn.Function("strftime", "%Y", Field("started_at"))[10:20]),
            ),
            compile_viz_spec(
                VizSpec(
                    x_axis=XAxis(
                        name="started_at",
                        binner=Binner(binner_type="datetime", time_unit="year"),
                        domain=[10, 20],
                    ),
                    tables=["customers"],
                )
            ),
        )

    def test_select_x_axis_domain_between_computed_alias(self):
        self.assertTreeEquals(
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
            compile_viz_spec(
                VizSpec(
                    x_axis=XAxis(
                        name="started_at",
                        alias="started_at_year",
                        binner=Binner(binner_type="datetime", time_unit="year"),
                        domain=[10, 20],
                    ),
                    tables=["customers"],
                )
            ),
        )

    def test_select_x_axis_domain_between_computed_alias_irderby(self):
        self.assertTreeEquals(
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
            compile_viz_spec(
                VizSpec(
                    x_axis=XAxis(
                        name="started_at",
                        alias="started_at_year",
                        binner=Binner(binner_type="datetime", time_unit="year"),
                        domain=[10, 20],
                    ),
                    sort_by=SortBy(name="started_at", direction="desc"),
                    tables=["customers"],
                )
            ),
        )

    def test_select_filter_simple_eq(self):
        self.assertTreeEquals(
            SqlTree(
                select_terms=["*"],
                from_term="customers",
                where_term=(Field("foo") == "bar"),
            ),
            compile_viz_spec(
                VizSpec(
                    y_axises=[YAxis(name="*")],
                    tables=["customers"],
                    filters=[
                        Filter(name="foo", filter_type="comparison", values=["bar"])
                    ],
                )
            ),
        )

    def test_select_filter_simple_not_eq(self):
        self.assertTreeEquals(
            SqlTree(
                select_terms=["*"],
                from_term="customers",
                where_term=(Field("foo") == "bar").negate(),
            ),
            compile_viz_spec(
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
                )
            ),
        )

    def test_select_filter_simple_eq_null(self):
        self.assertTreeEquals(
            SqlTree(
                select_terms=["*"],
                from_term="customers",
                where_term=(Field("foo").isnull()),
            ),
            compile_viz_spec(
                VizSpec(
                    y_axises=[YAxis(name="*")],
                    tables=["customers"],
                    filters=[
                        Filter(name="foo", filter_type="comparison", values=["null"])
                    ],
                )
            ),
        )

    def test_select_filter_simple_not_eq(self):
        self.assertTreeEquals(
            SqlTree(
                select_terms=["*"],
                from_term="customers",
                where_term=(Field("foo")).notnull(),
            ),
            compile_viz_spec(
                VizSpec(
                    y_axises=[YAxis(name="*")],
                    tables=["customers"],
                    filters=[
                        Filter(
                            name="foo",
                            filter_type="comparison",
                            values=["null"],
                            negate=True,
                        )
                    ],
                )
            ),
        )

    def test_select_filter_like(self):
        self.assertTreeEquals(
            SqlTree(
                select_terms=["*"],
                from_term="customers",
                where_term=(Field("foo").like("%MC%")),
            ),
            compile_viz_spec(
                VizSpec(
                    y_axises=[YAxis(name="*")],
                    tables=["customers"],
                    filters=[Filter(name="foo", filter_type="like", values=["%MC%"])],
                ),
            ),
        )

    def test_select_filter_like_negate(self):
        self.assertTreeEquals(
            SqlTree(
                select_terms=["*"],
                from_term="customers",
                where_term=(Field("foo").like("%MC%")).negate(),
            ),
            compile_viz_spec(
                VizSpec(
                    y_axises=[YAxis(name="*")],
                    tables=["customers"],
                    filters=[
                        Filter(
                            name="foo", filter_type="like", values=["%MC%"], negate=True
                        )
                    ],
                ),
            ),
        )

    def test_select_or_filter_2(self):
        self.assertTreeEquals(
            SqlTree(
                select_terms=["*"],
                from_term="customers",
                where_term=(
                    (Field("department") == "foo") | (Field("department") == "bar")
                ),
            ),
            compile_viz_spec(
                VizSpec(
                    y_axises=[YAxis(name="*")],
                    tables=["customers"],
                    filters=[
                        Filter(
                            name="department",
                            filter_type="complex",
                            expression="department = 'foo' OR department = 'bar'",
                        )
                    ],
                ),
            ),
        )

    def test_select_or_filter_bad_expression(self):
        compile_viz_spec(
            VizSpec(
                y_axises=[YAxis(name="*")],
                tables=["customers"],
                filters=[
                    Filter(
                        name="department",
                        filter_type="complex",
                        expression="asdf",
                    )
                ],
            ),
        )

    def test_select_or_filter_bad_expression_2(self):
        compile_viz_spec(
            VizSpec(
                y_axises=[YAxis(name="*")],
                tables=["customers"],
                filters=[
                    Filter(
                        name="department",
                        filter_type="complex",
                        expression="a + 10",
                    )
                ],
            )
        )

    def test_allows_and_expression(self):
        compile_viz_spec(
            VizSpec(
                y_axises=[YAxis(name="*")],
                tables=["customers"],
                filters=[
                    Filter(
                        name="department",
                        filter_type="complex",
                        expression="a > 10 and b < 10",
                    )
                ],
            )
        )

    def test_select_filter_simple_in(self):
        self.assertTreeEquals(
            SqlTree(
                select_terms=["*"],
                from_term="customers",
                where_term=(Field("foo").isin(["bar", "bar2"])),
            ),
            compile_viz_spec(
                VizSpec(
                    y_axises=[YAxis(name="*")],
                    tables=["customers"],
                    filters=[
                        Filter(
                            name="foo", filter_type="comparison", values=["bar", "bar2"]
                        )
                    ],
                )
            ),
        )

    def test_select_filter_simple_gt(self):
        self.assertTreeEquals(
            SqlTree(
                select_terms=["*"],
                from_term="customers",
                where_term=(Field("foo") > 10),
            ),
            compile_viz_spec(
                VizSpec(
                    y_axises=[YAxis(name="*")],
                    tables=["customers"],
                    filters=[
                        Filter(name="foo", filter_type="numeric", domain=[10, None])
                    ],
                )
            ),
        )

    def test_select_filter_simple_gt_negate(self):
        self.assertTreeEquals(
            SqlTree(
                select_terms=["*"],
                from_term="customers",
                where_term=(Field("foo") > 10).negate(),
            ),
            compile_viz_spec(
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
                )
            ),
        )

    def test_select_filter_simple_lt(self):
        self.assertTreeEquals(
            SqlTree(
                select_terms=["*"],
                from_term="customers",
                where_term=(Field("foo") < 10),
            ),
            compile_viz_spec(
                VizSpec(
                    y_axises=[YAxis(name="*")],
                    tables=["customers"],
                    filters=[
                        Filter(name="foo", filter_type="numeric", domain=[None, 10])
                    ],
                )
            ),
        )

    def test_select_filter_simple_lt_negate(self):
        self.assertTreeEquals(
            SqlTree(
                select_terms=["*"],
                from_term="customers",
                where_term=(Field("foo") < 10).negate(),
            ),
            compile_viz_spec(
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
                )
            ),
        )

    def test_select_filter_simple_between(self):
        self.assertTreeEquals(
            SqlTree(
                select_terms=["*"],
                from_term="customers",
                where_term=(Field("foo")[10:20]),
            ),
            compile_viz_spec(
                VizSpec(
                    y_axises=[YAxis(name="*")],
                    tables=["customers"],
                    filters=[
                        Filter(name="foo", filter_type="numeric", domain=[10, 20])
                    ],
                )
            ),
        )

    def test_select_filter_simple_between_negate(self):
        self.assertTreeEquals(
            SqlTree(
                select_terms=["*"],
                from_term="customers",
                where_term=(Field("foo")[10:20]).negate(),
            ),
            compile_viz_spec(
                VizSpec(
                    y_axises=[YAxis(name="*")],
                    tables=["customers"],
                    filters=[
                        Filter(
                            name="foo",
                            filter_type="numeric",
                            domain=[10, 20],
                            negate=True,
                        )
                    ],
                )
            ),
        )

    def test_select_filter_multiple(self):
        self.assertTreeEquals(
            SqlTree(
                select_terms=["*"],
                from_term="customers",
                where_term=((Field("foo")[10:20]) & (Field("bar") == "asdf")),
            ),
            compile_viz_spec(
                VizSpec(
                    y_axises=[YAxis(name="*")],
                    tables=["customers"],
                    filters=[
                        Filter(name="foo", filter_type="numeric", domain=[10, 20]),
                        Filter(name="bar", filter_type="comparison", values=["asdf"]),
                    ],
                )
            ),
        )

    def test_select_filter_multiple_one_negate(self):
        self.assertTreeEquals(
            SqlTree(
                select_terms=["*"],
                from_term="customers",
                where_term=((Field("foo")[10:20]).negate() & (Field("bar") == "asdf")),
            ),
            compile_viz_spec(
                VizSpec(
                    y_axises=[YAxis(name="*")],
                    tables=["customers"],
                    filters=[
                        Filter(
                            name="foo",
                            filter_type="numeric",
                            domain=[10, 20],
                            negate=True,
                        ),
                        Filter(name="bar", filter_type="comparison", values=["asdf"]),
                    ],
                )
            ),
        )

    def test_select_filter_multiple_x_axis_domain(self):
        self.assertTreeEquals(
            SqlTree(
                select_terms=[Field("a"), fn.Count("*")],
                from_term="customers",
                groupby_terms=[Field("a")],
                where_term=(
                    (Field("a")[10:20])
                    & (Field("foo")[10:20])
                    & (Field("bar") == "asdf")
                ),
            ),
            compile_viz_spec(
                VizSpec(
                    x_axis=XAxis(name="a", domain=[10, 20]),
                    y_axises=[YAxis(name="*", aggregation="COUNT")],
                    tables=["customers"],
                    filters=[
                        Filter(name="foo", filter_type="numeric", domain=[10, 20]),
                        Filter(name="bar", filter_type="comparison", values=["asdf"]),
                    ],
                )
            ),
        )

    def test_select_groupby_func(self):
        self.assertTreeEquals(
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
            compile_viz_spec(
                VizSpec(
                    x_axis=XAxis(
                        name="started_at",
                        binner=Binner(binner_type="datetime", time_unit="day"),
                    ),
                    y_axises=[YAxis(name="*", aggregation="COUNT", alias="num_trips")],
                    tables=["JC_202307_citibike_tripdata"],
                    limit=500,
                )
            ),
        )

    def test_top_market_segments_subquery(self):
        self.assertTreeEquals(
            SqlTree(
                select_terms=[
                    Field("COUNT(*) * 100 / (select COUNT(*) from ORDERS)").as_(
                        "percentage_returned"
                    )
                ],
                from_term="ORDERS",
                where_term=(Field("O_ORDERSTATUS") == "RETURNED"),
                limit_term=500,
            ),
            compile_viz_spec(
                VizSpec(
                    y_axises=[
                        YAxis(
                            name="COUNT(*) * 100 / (select COUNT(*) from ORDERS)",
                            alias="percentage_returned",
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
                )
            ),
        )

    def test_unparsed_subtree(self):
        self.assertTreeEquals(
            SqlTree(
                select_terms=[
                    fn.AggregateFunction(
                        "AVG",
                        LiteralValue(
                            "strftime('%s',ended_at) - strftime('%s',started_at)"
                        ),
                    ).as_("average_duration")
                ],
                from_term="citbike_partial_demo",
            ),
            compile_viz_spec(
                VizSpec(
                    y_axises=[
                        YAxis(
                            name="strftime('%s',ended_at) - strftime('%s',started_at)",
                            aggregation="AVG",
                            alias="average_duration",
                            unparsed=True,
                        )
                    ],
                    tables=["citbike_partial_demo"],
                )
            ),
        )

    def test_select_join_on(self):
        compiler = SqliteCompiler(
            DatabaseSchema(
                sql_dialect="Sqlite",
                foreign_keys=[
                    ForeignKey(primary="customers.id", reference="orders.id")
                ],
            )
        )
        self.assertTreeEquals(
            SqlTree(
                select_terms=["*"],
                from_term="customers",
                joinon_terms=[
                    (
                        Table("orders"),
                        Field("customers.id") == Field("orders.id"),
                    )
                ],
            ),
            compiler.compile(
                VizSpec(
                    y_axises=[YAxis(name="*")],
                    tables=["customers", "orders"],
                ),
            ),
        )

    def test_select_join_multiple(self):
        compiler = SqliteCompiler(
            DatabaseSchema(
                sql_dialect="Sqlite",
                foreign_keys=[
                    ForeignKey(primary="customers.id", reference="orders.id"),
                    ForeignKey(primary="orders.id", reference="lineitems.id"),
                ],
            )
        )
        self.assertTreeEquals(
            SqlTree(
                select_terms=["*"],
                from_term="customers",
                joinon_terms=[
                    (
                        Table("orders"),
                        Field("customers.id") == Field("orders.id"),
                    ),
                    (
                        Table("lineitems"),
                        Field("orders.id") == Field("lineitems.id"),
                    ),
                ],
            ),
            compiler.compile(
                VizSpec(
                    y_axises=[YAxis(name="*")],
                    tables=["customers", "orders", "lineitems"],
                ),
            ),
        )
