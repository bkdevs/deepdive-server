from pypika import functions as fn
from pypika.terms import Field

from deepdive.sql.parser.sql_tree import SqlTree
from deepdive.test.sql.sql_test_case import (
    SqlTestCase,
    compile_viz_spec,
    generate_viz_spec,
)


class TestVizInteg(SqlTestCase):
    """
    Tests that we can generate a VizSpec given a SqlTree and back

    Note that this behavior _isn't_ identical

    As in there are cases where doing this translation will munge the SqlTree
    In particular, we impose a certain order of select columns in the query

    As well as append all group by columns _into_ the select columns if not present
    see tests without assertConversionIdentical for an example of the behavior we enforce
    """

    def assertConversionIdentical(self, sql_tree: SqlTree):
        self.assertTreeEquals(sql_tree, compile_viz_spec(generate_viz_spec(sql_tree)))

    def test_simple_select(self):
        self.assertConversionIdentical(
            SqlTree(select_terms=[Field("a")], from_term="customers")
        )

    def test_simple_select_two_columns(self):
        self.assertConversionIdentical(
            SqlTree(select_terms=[Field("a"), Field("b")], from_term="customers")
        )

    def test_simple_select_two_columns_orderby_one(self):
        self.assertConversionIdentical(
            SqlTree(
                select_terms=[Field("a"), Field("b")],
                from_term="customers",
                orderby_term=(Field("a"), "ASC"),
            )
        )

    def test_simple_select_star(self):
        self.assertConversionIdentical(
            SqlTree(select_terms=["*"], from_term="customers")
        )

    def test_select_limit(self):
        self.assertConversionIdentical(
            SqlTree(select_terms=[Field("a")], from_term="customers", limit_term=500)
        )

    def test_select_group_by(self):
        self.assertConversionIdentical(
            SqlTree(
                select_terms=[Field("a")],
                groupby_terms=[Field("a")],
                from_term="customers",
            )
        )

    def test_select_group_by_alias(self):
        self.assertConversionIdentical(
            SqlTree(
                select_terms=[Field("a").as_("b")],
                groupby_terms=[Field("b")],
                from_term="customers",
            )
        )

    def test_select_group_by_xaxis(self):
        self.assertConversionIdentical(
            SqlTree(
                select_terms=[Field("a")],
                groupby_terms=[Field("a")],
                from_term="customers",
            )
        )

    def test_select_group_by_xaxis_alias(self):
        self.assertConversionIdentical(
            SqlTree(
                select_terms=[Field("a").as_("b")],
                groupby_terms=[Field("b")],
                from_term="customers",
            )
        )

    def test_select_group_by_limit(self):
        self.assertConversionIdentical(
            SqlTree(
                select_terms=[Field("a")],
                groupby_terms=[Field("a")],
                from_term="customers",
                limit_term=100,
            )
        )

    def test_select_few_group_by_limit(self):
        self.assertConversionIdentical(
            SqlTree(
                select_terms=[Field("a"), fn.Count("*")],
                groupby_terms=[Field("a")],
                from_term="customers",
                limit_term=100,
            )
        )

    def test_select_few_group_by_few_limit(self):
        self.assertConversionIdentical(
            SqlTree(
                select_terms=[Field("a"), Field("b")],
                groupby_terms=[Field("a"), Field("b")],
                from_term="customers",
                limit_term=100,
            )
        )

    def test_select_aggregated_y(self):
        # this fails since we re-order select columns (i.e, x axis, breakdowns, first)
        # that's okay, so explicitly coding in what we expect us to do here

        sql_tree = SqlTree(
            select_terms=[
                fn.Count("*"),
                Field("a"),
                Field("b"),
            ],  # this will fail integ since we append right now, probably okay?
            groupby_terms=[Field("a"), Field("b")],
            from_term="customers",
            limit_term=100,
        )
        converted_tree = compile_viz_spec(generate_viz_spec(sql_tree))
        expected_tree = SqlTree(
            select_terms=[
                Field("a"),
                Field("b"),
                fn.Count("*"),
            ],
            groupby_terms=[Field("a"), Field("b")],
            from_term="customers",
            limit_term=100,
        )
        self.assertTreeEquals(expected_tree, converted_tree)

    def test_select_aggregated_y_no_groupbys(self):
        # this fails since we append all groupby columns, breakdown columns if not present in the select
        # that's okay (probably good really) but let's test for that behavior here
        sql_tree = SqlTree(
            select_terms=[fn.Count("*")],
            groupby_terms=[Field("a"), Field("b")],
            from_term="customers",
            limit_term=100,
        )
        converted_tree = compile_viz_spec(generate_viz_spec(sql_tree))
        expected_tree = SqlTree(
            select_terms=[Field("a"), Field("b"), fn.Count("*")],
            groupby_terms=[Field("a"), Field("b")],
            from_term="customers",
            limit_term=100,
        )
        self.assertTreeEquals(expected_tree, converted_tree)

    def test_select_count_star(self):
        self.assertConversionIdentical(
            SqlTree(select_terms=[fn.Count("*")], from_term="customers")
        )

    def test_select_count_star_alias(self):
        self.assertConversionIdentical(
            SqlTree(select_terms=[fn.Count("*").as_("num_rows")], from_term="customers")
        )

    def test_select_count_asdf(self):
        self.assertConversionIdentical(
            SqlTree(select_terms=[fn.Count("asdf")], from_term="customers"),
        )

    def test_select_avg_star(self):
        self.assertConversionIdentical(
            SqlTree(
                select_terms=[fn.AggregateFunction("AVG", Field("customerVal"))],
                from_term="customers",
            ),
        )

    def test_select_min(self):
        self.assertConversionIdentical(
            SqlTree(
                select_terms=[fn.AggregateFunction("MIN", Field("customerVal"))],
                from_term="customers",
            ),
        )

    def test_select_count_avg(self):
        self.assertConversionIdentical(
            SqlTree(
                select_terms=[
                    fn.Count("*"),
                    fn.AggregateFunction("AVG", Field("customerVal")),
                ],
                from_term="customers",
            ),
        )

    def test_select_datetime_x(self):
        self.assertConversionIdentical(
            SqlTree(
                select_terms=[
                    fn.Function("strftime", "%Y-%m-%d %H:%M:%S", Field("started_at"))
                ],
                groupby_terms=[
                    fn.Function("strftime", "%Y-%m-%d %H:%M:%S", Field("started_at"))
                ],
                from_term="customers",
            ),
        )

    def test_select_datetime_x_alias(self):
        self.assertConversionIdentical(
            SqlTree(
                select_terms=[
                    fn.Function(
                        "strftime", "%Y-%m-%d %H:%M:%S", Field("started_at")
                    ).as_("started_at_seconds")
                ],
                groupby_terms=[Field("started_at_seconds")],
                from_term="customers",
            ),
        )

    def test_select_datetime_x_minutes(self):
        self.assertConversionIdentical(
            SqlTree(
                select_terms=[
                    fn.Function("strftime", "%Y-%m-%d %H:%M", Field("started_at"))
                ],
                groupby_terms=[
                    fn.Function("strftime", "%Y-%m-%d %H:%M", Field("started_at"))
                ],
                from_term="customers",
            ),
        )

    def test_select_datetime_x_hours(self):
        self.assertConversionIdentical(
            SqlTree(
                select_terms=[
                    fn.Function("strftime", "%Y-%m-%d %H", Field("started_at"))
                ],
                groupby_terms=[
                    fn.Function("strftime", "%Y-%m-%d %H", Field("started_at"))
                ],
                from_term="customers",
            ),
        )

    def test_select_datetime_x_days(self):
        self.assertConversionIdentical(
            SqlTree(
                select_terms=[fn.Function("strftime", "%Y-%m-%d", Field("started_at"))],
                groupby_terms=[
                    fn.Function("strftime", "%Y-%m-%d", Field("started_at"))
                ],
                from_term="customers",
            ),
        )

    def test_select_datetime_x_weeks(self):
        self.assertConversionIdentical(
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
        )

    def test_select_datetime_x_months(self):
        self.assertConversionIdentical(
            SqlTree(
                select_terms=[fn.Function("strftime", "%Y-%m", Field("started_at"))],
                groupby_terms=[fn.Function("strftime", "%Y-%m", Field("started_at"))],
                from_term="customers",
            ),
        )

    def test_select_datetime_x_years(self):
        self.assertConversionIdentical(
            SqlTree(
                select_terms=[fn.Function("strftime", "%Y", Field("started_at"))],
                groupby_terms=[fn.Function("strftime", "%Y", Field("started_at"))],
                from_term="customers",
            ),
        )

    def test_select_group_by_order_by(self):
        self.assertConversionIdentical(
            SqlTree(
                select_terms=[Field("a")],
                groupby_terms=[Field("a")],
                from_term="customers",
                orderby_term=(Field("a"), "ASC"),
            ),
        )

    def test_select_group_by_order_by_desc(self):
        self.assertConversionIdentical(
            SqlTree(
                select_terms=[Field("a")],
                groupby_terms=[Field("a")],
                from_term="customers",
                orderby_term=(Field("a"), "DESC"),
            ),
        )

    def test_select_order_by_y_axis_aggregation(self):
        self.assertConversionIdentical(
            SqlTree(
                select_terms=[Field("a"), fn.Count("b")],
                groupby_terms=[Field("a")],
                from_term="customers",
                orderby_term=(Field("a"), "DESC"),
            ),
        )

    def test_select_order_by_y_axis_aggregation_star(self):
        self.assertConversionIdentical(
            SqlTree(
                select_terms=[Field("a"), fn.Count("*")],
                groupby_terms=[Field("a")],
                from_term="customers",
                orderby_term=(Field("a"), "DESC"),
            ),
        )

    def test_select_order_by_y_axis_count(self):
        self.assertConversionIdentical(
            SqlTree(
                select_terms=[Field("a"), fn.Count("b")],
                groupby_terms=[Field("a")],
                from_term="customers",
                orderby_term=(fn.Count("b"), "DESC"),
            ),
        )

    def test_select_order_by_y_axis_count_alias(self):
        self.assertConversionIdentical(
            SqlTree(
                select_terms=[Field("a"), fn.Count("b").as_("countb")],
                groupby_terms=[Field("a")],
                from_term="customers",
                orderby_term=(Field("countb"), "DESC"),
            ),
        )

    def test_select_order_by_x_axis_bin(self):
        self.assertConversionIdentical(
            SqlTree(
                select_terms=[fn.Function("strftime", "%Y", Field("started_at"))],
                groupby_terms=[fn.Function("strftime", "%Y", Field("started_at"))],
                orderby_term=(
                    fn.Function("strftime", "%Y", Field("started_at")),
                    "ASC",
                ),
                from_term="customers",
            )
        )

    def test_select_order_by_x_axis_bin_alias(self):
        self.assertConversionIdentical(
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
        )

    def test_select_order_by_breakdown(self):
        self.assertConversionIdentical(
            SqlTree(
                select_terms=[Field("a"), Field("b")],
                groupby_terms=[Field("a"), Field("b")],
                orderby_term=[Field("b"), "ASC"],
                from_term="customers",
            ),
        )

    def test_select_x_axis_domain_min(self):
        self.assertConversionIdentical(
            SqlTree(
                select_terms=[Field("a")],
                groupby_terms=[Field("a")],
                from_term="customers",
                where_term=(Field("a") >= 10),
            ),
        )

    def test_select_x_axis_domain_max(self):
        self.assertConversionIdentical(
            SqlTree(
                select_terms=[Field("a")],
                groupby_terms=[Field("a")],
                from_term="customers",
                where_term=(Field("a") <= 10),
            ),
        )

    def test_select_x_axis_domain_decimal_max(self):
        self.assertConversionIdentical(
            SqlTree(
                select_terms=[Field("a")],
                groupby_terms=[Field("a")],
                from_term="customers",
                where_term=(Field("a") <= 20.5),
            ),
        )

    def test_select_x_axis_domain_string(self):
        self.assertConversionIdentical(
            SqlTree(
                select_terms=[Field("a")],
                groupby_terms=[Field("a")],
                from_term="customers",
                where_term=(Field("a") <= "bar"),
            ),
        )

    def test_select_x_axis_domain_between(self):
        self.assertConversionIdentical(
            SqlTree(
                select_terms=[Field("a")],
                groupby_terms=[Field("a")],
                from_term="customers",
                where_term=(Field("a")[10:20]),
            ),
        )

    def test_select_x_axis_domain_between_computed(self):
        self.assertConversionIdentical(
            SqlTree(
                select_terms=[fn.Function("strftime", "%Y", Field("started_at"))],
                groupby_terms=[fn.Function("strftime", "%Y", Field("started_at"))],
                from_term="customers",
                where_term=(fn.Function("strftime", "%Y", Field("started_at"))[10:20]),
            ),
        )

    def test_select_x_axis_domain_between_computed_alias(self):
        self.assertConversionIdentical(
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
        )

    def test_select_x_axis_domain_between_computed_alias_irderby(self):
        self.assertConversionIdentical(
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
        )

    def test_select_filter_simple_eq(self):
        self.assertConversionIdentical(
            SqlTree(
                select_terms=["*"],
                from_term="customers",
                where_term=(Field("foo") == "bar"),
            ),
        )

    def test_select_filter_simple_eq_flipped(self):
        self.assertConversionIdentical(
            SqlTree(
                select_terms=["*"],
                from_term="customers",
                where_term=("bar" == Field("foo")),
            ),
        )

    def test_select_filter_simple_in(self):
        self.assertConversionIdentical(
            SqlTree(
                select_terms=["*"],
                from_term="customers",
                where_term=(Field("foo").isin(["bar", "bar2"])),
            ),
        )

    def test_select_filter_simple_gt(self):
        self.assertConversionIdentical(
            SqlTree(
                select_terms=["*"],
                from_term="customers",
                where_term=(Field("foo") > 10),
            ),
        )

    def test_select_filter_simple_lt(self):
        self.assertConversionIdentical(
            SqlTree(
                select_terms=["*"],
                from_term="customers",
                where_term=(Field("foo") < 10),
            ),
        )

    def test_select_filter_simple_gt_flipped(self):
        self.assertConversionIdentical(
            SqlTree(
                select_terms=["*"],
                from_term="customers",
                where_term=(10 > Field("foo")),
            ),
        )

    def test_select_filter_simple_lt_flipped(self):
        # pypika sanitizes where entries, so flipping results _does_ lead to an identical result
        self.assertConversionIdentical(
            SqlTree(
                select_terms=["*"],
                from_term="customers",
                where_term=(10 < Field("foo")),
            ),
        )

    def test_select_filter_simple_between(self):
        self.assertConversionIdentical(
            SqlTree(
                select_terms=["*"],
                from_term="customers",
                where_term=(Field("foo")[10:20]),
            ),
        )

    def test_select_filter_multiple(self):
        self.assertConversionIdentical(
            SqlTree(
                select_terms=["*"],
                from_term="customers",
                where_term=(Field("foo")[10:20] & (Field("bar") == "asdf")),
            ),
        )

    def test_select_filter_multiple_x_axis_domain(self):
        self.assertConversionIdentical(
            SqlTree(
                select_terms=[Field("a"), fn.Count("*")],
                from_term="customers",
                groupby_terms=[Field("a")],
                where_term=(
                    Field("a")[10:20] & Field("foo")[10:20] & (Field("bar") == "asdf")
                ),
            ),
        )

    def test_select_groupby_func(self):
        self.assertConversionIdentical(
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
        )

    def test_top_market_segments_subquery(self):
        self.assertConversionIdentical(
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
        )


#     def test_select_join_on(self):
#         query = "select * from customer join orders on customer.id = orders.id"
#         self.assertEqual(
#             SqlTree(
#                 select_terms=["*"],
#                 from_term="customer",
#                 joinon_term=(
#                     Table("orders"),
#                     Field("customer.id") == Field("orders.id"),
#                 ),
#             ),
#             parse_sql(query),
#         )

#     def test_select_join_on_capitalized(self):
#         query = "select * from customer join orders ON customer.id = orders.id"
#         self.assertEqual(
#             SqlTree(
#                 select_terms=["*"],
#                 from_term="customer",
#                 joinon_term=(
#                     Table("orders"),
#                     Field("customer.id") == Field("orders.id"),
#                 ),
#             ),
#             parse_sql(query),
#         )

#     def test_select_join_multiple(self):
#         query = "select * from customer join orders on customer.id = orders.id join lineitems on orders.id = lineitems.id"
#         self.assertEqual(
#             SqlTree(
#                 select_terms=["*"],
#                 from_term="customer",
#                 joinon_term=(
#                     Table("lineitems"),
#                     Field("orders.id") == Field("lineitems.id"),
#                 ),
#             ),
#             parse_sql(query),
#         )
