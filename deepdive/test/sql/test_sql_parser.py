from pypika import Table
from pypika import functions as fn
from pypika.terms import Field

from deepdive.sql.parser.term_parser import UnparsedField
from deepdive.sql.parser.sql_parser import parse_sql
from deepdive.sql.parser.sql_tree import SqlTree
from deepdive.test.sql.sql_test_case import SqlTestCase


class TestSqlParser(SqlTestCase):
    def test_simple_select(self):
        query = """ select a from customers """
        self.assertTreeEquals(
            SqlTree(select_terms=[Field("a")], from_term="customers"), parse_sql(query)
        )

    def test_raises_show_tables(self):
        with self.assertRaises(ValueError):
            query = """ show tables """
            parse_sql(query)

    def test_simple_select_two_columns(self):
        query = """ select a,b from customers """
        self.assertTreeEquals(
            SqlTree(select_terms=[Field("a"), Field("b")], from_term="customers"),
            parse_sql(query),
        )

    def test_simple_select_star(self):
        query = """ select * from customers """
        self.assertTreeEquals(
            SqlTree(select_terms=["*"], from_term="customers"),
            parse_sql(query),  # we can give wildcard as a string to pypika
        )

    def test_select_limit(self):
        query = """ select a from customers limit 500"""
        self.assertTreeEquals(
            SqlTree(select_terms=[Field("a")], from_term="customers", limit_term=500),
            parse_sql(query),
        )

    def test_select_group_by(self):
        query = """ select a from customers group by a"""
        self.assertTreeEquals(
            SqlTree(
                select_terms=[Field("a")],
                groupby_terms=[Field("a")],
                from_term="customers",
            ),
            parse_sql(query),
        )

    def test_select_group_by_limit(self):
        query = """ select a from customers group by a limit 100"""
        self.assertTreeEquals(
            SqlTree(
                select_terms=[Field("a")],
                groupby_terms=[Field("a")],
                from_term="customers",
                limit_term=100,
            ),
            parse_sql(query),
        )

    def test_select_few_group_by_limit(self):
        # note non-validating
        query = """ select a, b from customers group by a limit 100"""
        self.assertTreeEquals(
            SqlTree(
                select_terms=[Field("a"), Field("b")],
                groupby_terms=[Field("a")],
                from_term="customers",
                limit_term=100,
            ),
            parse_sql(query),
        )

    def test_select_few_group_by_few_limit(self):
        query = """ select a, b from customers group by a, b limit 100"""
        self.assertTreeEquals(
            SqlTree(
                select_terms=[Field("a"), Field("b")],
                groupby_terms=[Field("a"), Field("b")],
                from_term="customers",
                limit_term=100,
            ),
            parse_sql(query),
        )

    def test_select_start_groupby_limit(self):
        query = """ select * from customers group by a, b limit 100"""
        self.assertTreeEquals(
            SqlTree(
                select_terms=["*"],
                groupby_terms=[Field("a"), Field("b")],
                from_term="customers",
                limit_term=100,
            ),
            parse_sql(query),
        )

    def test_select_group_by_order_by(self):
        query = """ select a from customers group by a order by a asc, b"""
        self.assertTreeEquals(
            SqlTree(
                select_terms=[Field("a")],
                groupby_terms=[Field("a")],
                from_term="customers",
                orderby_term=(Field("a"), "ASC"),
            ),
            parse_sql(query),
        )

    def test_select_group_by_order_by_func(self):
        query = """ select COUNT(*) from customers order by COUNT(*)"""
        self.assertTreeEquals(
            SqlTree(
                select_terms=[fn.Count("*")],
                from_term="customers",
                orderby_term=(fn.Count("*"), "ASC"),
            ),
            parse_sql(query),
        )

    def test_select_group_by_order_by_asc(self):
        query = """ select a from customers group by a order by a asc"""
        self.assertTreeEquals(
            SqlTree(
                select_terms=[Field("a")],
                groupby_terms=[Field("a")],
                from_term="customers",
                orderby_term=(Field("a"), "ASC"),
            ),
            parse_sql(query),
        )

    def test_select_group_by_order_by_asc_upper(self):
        query = """ select a from customers group by a order by a ASC"""
        self.assertTreeEquals(
            SqlTree(
                select_terms=[Field("a")],
                groupby_terms=[Field("a")],
                from_term="customers",
                orderby_term=(Field("a"), "ASC"),
            ),
            parse_sql(query),
        )

    def test_select_group_by_order_by_desc(self):
        query = """ select a from customers group by a order by a desc"""
        self.assertTreeEquals(
            SqlTree(
                select_terms=[Field("a")],
                groupby_terms=[Field("a")],
                from_term="customers",
                orderby_term=(Field("a"), "DESC"),
            ),
            parse_sql(query),
        )

    def test_select_group_by_order_by_multiple(self):
        # note we only parse the first term
        query = """ select a from customers group by a order by a, b"""
        self.assertTreeEquals(
            SqlTree(
                select_terms=[Field("a")],
                groupby_terms=[Field("a")],
                from_term="customers",
                orderby_term=(Field("a"), "ASC"),
            ),
            parse_sql(query),
        )

    def test_select_group_by_order_by_multiple_desc(self):
        # note we only parse the first term
        query = """ select a from customers group by a order by a desc, b"""
        self.assertTreeEquals(
            SqlTree(
                select_terms=[Field("a")],
                groupby_terms=[Field("a")],
                from_term="customers",
                orderby_term=(Field("a"), "DESC"),
            ),
            parse_sql(query),
        )

    def test_select_group_by_order_by_multiple_desc_2(self):
        # note we only parse the first term
        query = """ select a from customers group by a order by a, b desc"""
        self.assertTreeEquals(
            SqlTree(
                select_terms=[Field("a")],
                groupby_terms=[Field("a")],
                from_term="customers",
                orderby_term=(Field("a"), "ASC"),
            ),
            parse_sql(query),
        )

    def test_select_group_by_order_by_multiple_desc_2_hard(self):
        # note we only parse the first term
        query = """ select a from customers group by a order by `column space` asc, b desc"""
        self.assertTreeEquals(
            SqlTree(
                select_terms=[Field("a")],
                groupby_terms=[Field("a")],
                from_term="customers",
                orderby_term=(Field("column space"), "ASC"),
            ),
            parse_sql(query),
        )

    def test_select_where_eq(self):
        query = "select a from customers where a = 'foo'"
        self.assertTreeEquals(
            SqlTree(
                select_terms=[Field("a")],
                from_term="customers",
                where_term=(Field("a") == "foo"),
            ),
            parse_sql(query),
        )

    def test_select_having_compare(self):
        query = "select a from customers group by a having COUNT(*) >= 5"
        self.assertTreeEquals(
            SqlTree(
                select_terms=[Field("a")],
                from_term="customers",
                groupby_terms=[Field("a")],
                having_term=(fn.Count("*") >= 5),
            ),
            parse_sql(query),
        )

    def test_select_where_not_eq(self):
        query = "select a from customers where a <> 'foo'"
        self.assertTreeEquals(
            SqlTree(
                select_terms=[Field("a")],
                from_term="customers",
                where_term=(Field("a") != "foo"),
            ),
            parse_sql(query),
        )

    def test_select_where_not_eq_2(self):
        query = "select a from customers where not a = 'foo'"
        self.assertTreeEquals(
            SqlTree(
                select_terms=[Field("a")],
                from_term="customers",
                where_term=(Field("a") == "foo").negate(),
            ),
            parse_sql(query),
        )

    def test_select_where_gt(self):
        query = "select a from customers where a > 10"
        self.assertTreeEquals(
            SqlTree(
                select_terms=[Field("a")],
                from_term="customers",
                where_term=(Field("a") > 10),
            ),
            parse_sql(query),
        )

    def test_select_where_not_gt(self):
        query = "select a from customers where not a > 10"
        self.assertTreeEquals(
            SqlTree(
                select_terms=[Field("a")],
                from_term="customers",
                where_term=(Field("a") > 10).negate(),
            ),
            parse_sql(query),
        )

    def test_select_where_lt(self):
        query = "select a from customers where a < 20"
        self.assertTreeEquals(
            SqlTree(
                select_terms=[Field("a")],
                from_term="customers",
                where_term=(Field("a") < 20),
            ),
            parse_sql(query),
        )

    def test_select_where_lt_negate(self):
        query = "select a from customers where not a < 20"
        self.assertTreeEquals(
            SqlTree(
                select_terms=[Field("a")],
                from_term="customers",
                where_term=(Field("a") < 20).negate(),
            ),
            parse_sql(query),
        )

    def test_select_where_lt_decimal(self):
        query = "select a from customers where a < 20.5"
        self.assertTreeEquals(
            SqlTree(
                select_terms=[Field("a")],
                from_term="customers",
                where_term=(Field("a") < 20.5),
            ),
            parse_sql(query),
        )

    def test_select_where_a_in(self):
        query = "select a from customers where a in ('foo', 'bar')"
        self.assertTreeEquals(
            SqlTree(
                select_terms=[Field("a")],
                from_term="customers",
                where_term=(Field("a").isin(("foo", "bar"))),
            ),
            parse_sql(query),
        )

    def test_select_where_a_in_negate(self):
        query = "select a from customers where a not in ('foo', 'bar')"
        self.assertTreeEquals(
            SqlTree(
                select_terms=[Field("a")],
                from_term="customers",
                where_term=(Field("a").isin(("foo", "bar"))).negate(),
            ),
            parse_sql(query),
        )

    def test_select_where_between(self):
        query = "select a from customers where a not between 10 and 20"
        self.assertTreeEquals(
            SqlTree(
                select_terms=[Field("a")],
                from_term="customers",
                where_term=(Field("a")[10:20]).negate(),
            ),
            parse_sql(query),
        )

    def test_select_where_between(self):
        query = "select a from customers where a between 10 and 20"
        self.assertTreeEquals(
            SqlTree(
                select_terms=[Field("a")],
                from_term="customers",
                where_term=(Field("a")[10:20]),
            ),
            parse_sql(query),
        )

    def test_select_where_between_and(self):
        query = "select a from customers where a between 10 and 20 and b = 'foo'"
        self.assertTreeEquals(
            SqlTree(
                select_terms=[Field("a")],
                from_term="customers",
                where_term=(Field("a")[10:20] & (Field("b") == "foo")),
            ),
            parse_sql(query),
        )

    def test_table_alias(self):
        query = "select a from data Table_Data"
        self.assertTreeEquals(
            SqlTree(
                select_terms=[Field("a")],
                from_term=Table("data").as_("Table_Data"),
            ),
            parse_sql(query),
        )

    def test_table_alias_2(self):
        query = "select a from ORDERS o"
        self.assertTreeEquals(
            SqlTree(
                select_terms=[Field("a")],
                from_term=Table("ORDERS").as_("o"),
            ),
            parse_sql(query),
        )

    def test_table_alias_as(self):
        query = "select a from data as Table_Data"
        self.assertTreeEquals(
            SqlTree(
                select_terms=[Field("a")],
                from_term=Table("data").as_("Table_Data"),
            ),
            parse_sql(query),
        )

    def test_select_count_star(self):
        query = "select COUNT(*) from customers"
        self.assertTreeEquals(
            SqlTree(select_terms=[fn.Count("*")], from_term="customers"),
            parse_sql(query),
        )

    def test_select_avg_star(self):
        query = "select AVG(customerVal) from customers"
        self.assertTreeEquals(
            SqlTree(
                select_terms=[fn.AggregateFunction("AVG", Field("customerVal"))],
                from_term="customers",
            ),
            parse_sql(query),
        )

    def test_select_min(self):
        query = "select MIN(customerVal) from customers"
        self.assertTreeEquals(
            SqlTree(
                select_terms=[fn.AggregateFunction("MIN", Field("customerVal"))],
                from_term="customers",
            ),
            parse_sql(query),
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
            parse_sql(query),
        )

    def test_select_func_date(self):
        query = """ select DATE(started_at) from customers """
        self.assertTreeEquals(
            SqlTree(
                select_terms=[fn.Function("DATE", Field("started_at"))],
                from_term="customers",
            ),
            parse_sql(query),
        )

    def test_select_custom_func_date(self):
        query = """ select strftime('%s', started_at) from customers """
        self.assertTreeEquals(
            SqlTree(
                select_terms=[fn.Function("strftime", "%s", Field("started_at"))],
                from_term="customers",
            ),
            parse_sql(query),
        )

    def test_select_groupby_func(self):
        query = """
select DATE(started_at) as date,
       COUNT(*) as num_trips
from JC_202307_citibike_tripdata
group by DATE(started_at)
limit 500"""
        self.assertTreeEquals(
            SqlTree(
                select_terms=[
                    fn.Function("DATE", Field("started_at")).as_("date"),
                    fn.Count("*").as_("num_trips"),
                ],
                from_term="JC_202307_citibike_tripdata",
                groupby_terms=[fn.Function("DATE", Field("started_at"))],
                limit_term=500,
            ),
            parse_sql(query),
        )

    def test_keyword_column_name(self):
        query = """select month
        from customers
        group by month
        limit 500"""
        self.assertTreeEquals(
            SqlTree(
                select_terms=[UnparsedField("month")],
                groupby_terms=[UnparsedField("month")],
                from_term="customers",
                limit_term=500,
            ),
            parse_sql(query),
        )

    def test_top_market_segments_subquery(self):
        query = """
        select COUNT(*) * 100 / (select COUNT(*) from ORDERS) as percentage_returned
        from ORDERS
        where O_ORDERSTATUS = 'RETURNED'
        limit 500"""

        self.assertTreeEquals(
            SqlTree(
                select_terms=[
                    UnparsedField("COUNT(*) * 100 / (select COUNT(*) from ORDERS)").as_(
                        "percentage_returned"
                    )
                ],
                from_term="ORDERS",
                where_term=(Field("O_ORDERSTATUS") == "RETURNED"),
                limit_term=500,
            ),
            parse_sql(query),
        )

    def test_select_from_join_where(self):
        query = """
        select C_NAME,
        SUM(O_TOTALPRICE) as TOTAL_PRICE
        from CUSTOMER
        join ORDERS on CUSTOMER.C_CUSTKEY = ORDERS.O_CUSTKEY
        group by C_NAME
        order by TOTAL_PRICE desc
        limit 10"""
        self.assertTreeEquals(
            SqlTree(
                select_terms=[
                    Field("C_NAME"),
                    fn.AggregateFunction("SUM", Field("O_TOTALPRICE")).as_(
                        "TOTAL_PRICE"
                    ),
                ],
                from_term="CUSTOMER",
                joinon_terms=[
                    (
                        Table("ORDERS"),
                        Field("CUSTOMER.C_CUSTKEY") == Field("ORDERS.O_CUSTKEY"),
                    )
                ],
                groupby_terms=[Field("C_NAME")],
                orderby_term=(Field("TOTAL_PRICE"), "DESC"),
                limit_term=10,
            ),
            parse_sql(query),
        )

    def test_select_join_on(self):
        query = "select * from customer join orders on customer.id = orders.id"
        self.assertTreeEquals(
            SqlTree(
                select_terms=["*"],
                from_term="customer",
                joinon_terms=[
                    (
                        Table("orders"),
                        Field("customer.id") == Field("orders.id"),
                    )
                ],
            ),
            parse_sql(query),
        )

    def test_select_join_on_capitalized(self):
        query = "select * from customer join orders ON customer.id = orders.id"
        self.assertTreeEquals(
            SqlTree(
                select_terms=["*"],
                from_term="customer",
                joinon_terms=[
                    (
                        Table("orders"),
                        Field("customer.id") == Field("orders.id"),
                    )
                ],
            ),
            parse_sql(query),
        )

    def test_select_join_no_on(self):
        query = "select * from customer join orders"
        with self.assertRaises(ValueError):
            parse_sql(query)

    def test_select_as_upper(self):
        query = """ select a AS alias_a from customers """
        self.assertTreeEquals(
            SqlTree(select_terms=[Field("a").as_("alias_a")], from_term="customers"),
            parse_sql(query),
        )

    def test_citibike_orderby_desc(self):
        query = """SELECT start_station_name, COUNT(*) as count FROM citibike_partial_demo GROUP BY start_station_name ORDER BY count DESC LIMIT 10"""
        self.assertTreeEquals(
            SqlTree(
                select_terms=[Field("start_station_name"), fn.Count("*").as_("count")],
                from_term="citibike_partial_demo",
                groupby_terms=[Field("start_station_name")],
                orderby_term=[Field("count"), "DESC"],
                limit_term=10,
            ),
            parse_sql(query),
        )

    def test_quoted_columns(self):
        query = """
select strftime('%Y', `Hire_Date`) as `Hire_Date by year`,
       `Department`,
       COUNT(*) as `COUNT(*)`
from `data`
group by `Hire_Date by year`,
         `Department`
limit 500
"""

        self.assertTreeEquals(
            SqlTree(
                select_terms=[
                    fn.Function("strftime", "%Y", Field("Hire_Date")).as_(
                        "Hire_Date by year"
                    ),
                    Field("Department"),
                    fn.Count("*").as_("COUNT(*)"),
                ],
                from_term="data",
                groupby_terms=[Field("Hire_Date by year"), Field("Department")],
                limit_term=500,
            ),
            parse_sql(query),
        )

    def test_select_join_multiple_(self):
        query = """
select * from customer 
join orders on customer.id = orders.id 
join lineitems on orders.id = lineitems.id
"""

        self.assertTreeEquals(
            SqlTree(
                select_terms=["*"],
                from_term="customer",
                joinon_terms=[
                    (Table("orders"), Field("customer.id") == Field("orders.id")),
                    (
                        Table("lineitems"),
                        Field("orders.id") == Field("lineitems.id"),
                    ),
                ],
            ),
            parse_sql(query),
        )

    def test_join_multiple_alias(self):
        query = """
select *
from ORDERS o
join LINEITEM l on o.O_ORDERKEY = l.L_ORDERKEY
join CUSTOMER c on o.O_CUSTKEY = c.C_CUSTKEY
"""

        self.assertTreeEquals(
            SqlTree(
                select_terms=["*"],
                from_term=Table("ORDERS").as_("o"),
                joinon_terms=[
                    (
                        Table("LINEITEM").as_("l"),
                        Field("o.O_ORDERKEY") == Field("l.L_ORDERKEY"),
                    ),
                    (
                        Table("CUSTOMER").as_("c"),
                        Field("o.O_CUSTKEY") == Field("c.C_CUSTKEY"),
                    ),
                ],
            ),
            parse_sql(query),
        )

    def test_biketrips(self):
        query = """ SELECT count(*) * 100 / (select count(*) from bikeshare_trips) from bikeshare_trips where end_station_name = 'stolen' """

    def test_subquery_where(self):
        query = """
SELECT COUNT(*) FROM EmployeeData WHERE Hire_Date < (SELECT Hire_Date FROM EmployeeData WHERE Full_Name = 'easton bailey')
"""
        self.assertTreeEquals(
            SqlTree(
                select_terms=[fn.Count("*")],
                from_term="EmployeeData",
                where_term=UnparsedField(
                    "Hire_Date < (select Hire_Date from EmployeeData where Full_Name = 'easton bailey')"
                ),
            ),
            parse_sql(query),
        )
