import unittest

from deepdive.schema import DatabaseSchema, ForeignKey
from deepdive.sql.parser import parse_sql
from deepdive.test.sql.sql_test_case import (
    SqlTestCase,
    compile_viz_spec,
    generate_viz_spec,
)
from deepdive.viz.compiler.sqlite_compiler import SqliteCompiler


class TestSqlVizInteg(SqlTestCase):
    """
    Tests that we can take a string, translate it to a SqlTree, generate a viz spec, compile a viz spec, and build the string
    as an end-to-end test of all of our parsing layers.

    i.e,
     > string -> SqlTree -> VizSpec -> SqlTree -> string
    """

    def assert_conversion_identical(self, query):
        self.assert_sql_str_equal(
            query, compile_viz_spec(generate_viz_spec(parse_sql(query))).build_str()
        )

    def convert_query(self, query) -> str:
        return compile_viz_spec(generate_viz_spec(parse_sql(query))).build_str()

    def test_top_market_segments(self):
        query = """
select C_MKTSEGMENT,
       COUNT(*) as segment_count
from CUSTOMER
group by C_MKTSEGMENT
order by segment_count desc
limit 5"""
        self.assert_conversion_identical(query)

    def test_top_customers(self):
        query = """
select C_NAME,
       SUM(O_TOTALPRICE) as TOTAL_PRICE
from CUSTOMER
join ORDERS on CUSTOMER.C_CUSTKEY = ORDERS.O_CUSTKEY
group by C_NAME
order by TOTAL_PRICE desc
limit 10"""

        compiler = SqliteCompiler(
            DatabaseSchema(
                sql_dialect="Sqlite",
                foreign_keys=[
                    ForeignKey(
                        primary="CUSTOMER.C_CUSTKEY", reference="ORDERS.O_CUSTKEY"
                    )
                ],
            )
        )
        self.assert_sql_str_equal(
            query, compiler.compile(generate_viz_spec(parse_sql(query))).build_str()
        )

    def test_percentage_returned(self):
        query = """
select COUNT(*) * 100 /
  (select COUNT(*)
   from ORDERS) as percentage_returned
from ORDERS
where O_ORDERSTATUS = 'RETURNED'
limit 500"""
        self.assert_conversion_identical(query)

    def test_bad_o_discount(self):
        query = """
select O_ORDERKEY,
       O_ORDERDATE,
       O_TOTALPRICE,
       O_DISCOUNT
from ORDERS
where O_DISCOUNT > 0"""
        self.assert_conversion_identical(query)

    def test_get_customers(self):
        query = """
select C_CUSTKEY,
       C_NAME,
       C_ADDRESS,
       C_NATIONKEY,
       C_PHONE,
       C_ACCTBAL,
       C_MKTSEGMENT,
       C_COMMENT
from CUSTOMER
limit 500"""
        self.assert_conversion_identical(query)

    def test_select_star(self):
        query = """
select *
from JC_202307_citibike_tripdata
limit 500"""
        self.assert_conversion_identical(query)

    def test_get_string_like(self):
        query = """
select *
from JC_202307_citibike_tripdata
where start_station_name LIKE '%Manhattan%'
  or end_station_name LIKE '%Manhattan%'
limit 500"""
        self.assert_conversion_identical(query)

    def test_popular_bike_routes(self):
        query = """
select start_station_name,
       end_station_name,
       COUNT(*) as num_trips
from JC_202307_citibike_tripdata
group by start_station_name,
         end_station_name
order by num_trips desc
limit 10"""
        self.assert_conversion_identical(query)

    def test_hallucinate_zipcodes(self):
        query = """
select JC_202307_citibike_tripdata.*,
       zipcode
from JC_202307_citibike_tripdata
join zipcodes on JC_202307_citibike_tripdata.start_lat = zipcodes.lat"""

        compiler = SqliteCompiler(
            DatabaseSchema(
                sql_dialect="Sqlite",
                foreign_keys=[
                    ForeignKey(
                        primary="JC_202307_citibike_tripdata.start_lat",
                        reference="zipcodes.lat",
                    )
                ],
            )
        )
        self.assert_sql_str_equal(
            query, compiler.compile(generate_viz_spec(parse_sql(query))).build_str()
        )

    def test_daily_trips(self):
        query = """
select DATE(started_at) as date,
       COUNT(*) as num_trips
from JC_202307_citibike_tripdata
group by DATE(started_at)
limit 500"""
        expected_query = """
select strftime('%Y-%m-%d', started_at),
       COUNT(*) as num_trips
from JC_202307_citibike_tripdata
group by strftime('%Y-%m-%d', started_at)
limit 500"""
        # alias resolution gets borked because of moving date -> strftime
        self.assert_sql_str_equal(expected_query, self.convert_query(query))

    def test_top_trips_july_16th(self):
        query = """
select ride_id,
       rideable_type,
       started_at,
       ended_at,
       start_station_name,
       start_station_id,
       end_station_name,
       end_station_id,
       start_lat,
       start_lng,
       end_lat,
       end_lng,
       member_casual
from JC_202307_citibike_tripdata
where started_at >= '2023-07-16 00:00:00'
  and ended_at <= '2023-07-16 23:59:59'
order by started_at desc
limit 10"""
        expected_query = """
select ride_id,
       rideable_type,
       started_at,
       ended_at,
       start_station_name,
       start_station_id,
       end_station_name,
       end_station_id,
       start_lat,
       start_lng,
       end_lat,
       end_lng,
       member_casual
from JC_202307_citibike_tripdata
where started_at > '2023-07-16 00:00:00'
  and ended_at < '2023-07-16 23:59:59'
order by started_at desc
limit 10"""
        # expected difference since our domain truncates right now
        # and doesn't allow for specification of inclusive / exclusive ends
        self.assert_sql_str_equal(expected_query, self.convert_query(query))

    def subquery_percentage(self):
        query = """
select start_station_name,
       end_station_name,
       COUNT(*) /
  (select COUNT(*) from JC_202307_citibike_tripdata) as percentage
from JC_202307_citibike_tripdata
group by start_station_name,
         end_station_name
order by percentage desc
limit 10"""
        self.assert_conversion_identical(query)

    def more_subquery_percentage(self):
        query = """
select start_station_name,
       end_station_name,
       (COUNT(*) * 100.0 / (select COUNT(*) from JC_202307_citibike_tripdata)) as percentage
from JC_202307_citibike_tripdata
group by start_station_name,
         end_station_name
order by percentage desc
limit 100"""
        self.assert_conversion_identical(query)

    def trips_by_day_of_week(self):
        query = """
select strftime('%w', started_at) as day_of_week,
       count(*) as num_trips
from JC_202307_citibike_tripdata
group by day_of_week
limit 500"""
        self.assert_conversion_identical(query)

    def trips_in_minutes(self):
        query = """
select AVG((JulianDay(ended_at) - JulianDay(started_at)) * 24 * 60) as average_trip_duration_minutes
from JC_202307_citibike_tripdata
limit 500"""
        self.assert_conversion_identical(query)

    def average_trip_duration_strftime(self):
        query = """
select avg(strftime('%s', ended_at) - strftime('%s', started_at)) as average_duration
from citibike_partial_demo
limit 500"""
        self.assert_conversion_identical(query)

    def trip_distance(self):
        query = """
select AVG((6371 * acos(cos(radians(start_lat)) * cos(radians(end_lat)) * cos(radians(end_lng) - radians(start_lng)) + sin(radians(start_lat)) * sin(radians(end_lat)))) * 0.621371) as average_trip_distance
from JC_202307_citibike_tripdata
limit 500"""
        self.assert_conversion_identical(query)

    def hires_simple(self):
        query = """
select Ethnicity,
       COUNT(*) as Hires
from data
group by Ethnicity
limit 500"""
        self.assert_conversion_identical(query)

    def hires_groupby_percentages(self):
        query = """
select Age,
       Ethnicity,
       COUNT(*) as Hire_Count,
       (COUNT(*) * 100 /
          (select COUNT(*)
           from data)) as Percentage
from data
group by Age,
         Ethnicity
limit 500"""
        self.assert_conversion_identical(query)

    def test_gender_percentages(self):
        query = """select Gender,
       COUNT(*) as count,
       (COUNT(*) * 100 /
          (select COUNT(*)
           from data)) as Percentage
from data
group by Gender
limit 500"""
        self.assert_conversion_identical(query)

    def test_avg_salary(self):
        query = """
select Job_Title,
       AVG(Annual_Salary)
from data
group by Job_Title
limit 500"""
        self.assert_conversion_identical(query)

    def test_hires_by_month(self):
        query = """
select strftime('%Y-%m', Hire_Date) as month,
       COUNT(*) as Hires
from data
group by month
limit 500"""
        self.assert_conversion_identical(query)

    def test_country_city(self):
        query = """
select Country,
       City,
       COUNT(*) as Employee_Count
from data
group by Country,
         City
order by Employee_Count desc
limit 10"""
        self.assert_conversion_identical(query)

    def test_avg_max(self):
        query = """
select Department,
       AVG(Annual_Salary),
       MAX(Annual_Salary)
from data
group by Department
limit 500"""
        self.assert_conversion_identical(query)

    def test_select_where_contains(self):
        query = """
select *
from data
where Department in ('IT',
                     'Business',
                     'Marketing')
limit 500"""
        self.assert_conversion_identical(query)

    def test_select_where_or(self):
        query = """
select *
from data
where Department = 'IT'
  or Department = 'Business'
limit 500"""
        self.assert_conversion_identical(query)

    def test_table_alias(self):
        query = """
select *
from data Table_Data
where Department = 'IT'
  or Department = 'Business'
limit 500"""
        expected_query = """
select *
from data
where Department = 'IT'
  or Department = 'Business'
limit 500"""
        # NOTE: our parsing implicitly removes table aliases, this is okay and expected.
        self.assert_sql_str_equal(expected_query, self.convert_query(query))

    def test_table_alias_as(self):
        query = """
select *
from data as Table_Data
where Department = 'IT'
  or Department = 'Business'
limit 500"""
        expected_query = """
select *
from data
where Department = 'IT'
  or Department = 'Business'
limit 500"""

        # NOTE: our parsing implicitly removes table aliases, this is okay and expected.
        self.assert_sql_str_equal(expected_query, self.convert_query(query))

    def test_select_simple(self):
        query = "select a from customers"
        self.assert_conversion_identical(query)

    def test_select_simple_alias(self):
        query = "select a as b from customers"
        self.assert_conversion_identical(query)

    def test_select_simple_two_columns(self):
        query = "select a,b from customers"
        self.assert_conversion_identical(query)

    def test_select_simple_star(self):
        query = "select * from customers"
        self.assert_conversion_identical(query)

    def test_hire_query(self):
        query = "SELECT Hire_Date, COUNT(*) as Total_Hires FROM Data GROUP BY Hire_Date ORDER BY Hire_Date LIMIT 500"
        self.assert_conversion_identical(query)

    def test_strftime_week(self):
        query = "SELECT strftime('%Y-%m-%d', started_at, 'weekday 0', '-6 days') as week_start, count(*) as num_trips FROM citibike_partial_demo GROUP BY week_start ORDER BY week_start LIMIT 500"
        self.assert_conversion_identical(query)

    def test_average_trip_duration(self):
        query = """
select AVG((julianday(ended_at) - julianday(started_at)) * 24) as average_trip_duration
from citibike_partial_demo
limit 500
"""
        self.assert_conversion_identical(query)

    def test_trips_by_hour_of_day(self):
        query = """
select strftime('%H', started_at) as hour,
       COUNT(*) as total_trips
from citibike_partial_demo
group by hour
limit 500
"""
        self.assert_conversion_identical(query)

    def test_average_trip_duration_by_hour_of_day(self):
        query = """
select strftime('%H', started_at) as hour_of_day,
       AVG(strftime('%s', ended_at) - strftime('%s', started_at)) as avg_duration
from citibike_partial_demo
group by hour_of_day
limit 500
"""
        self.assert_conversion_identical(query)

    def test_trip_duration_by_day_of_week(self):
        query = """
select strftime('%w', started_at) as day_of_week,
       SUM(strftime('%s', ended_at) - strftime('%s', started_at)) as trip_duration
from citibike_partial_demo
group by day_of_week
limit 500
"""
        self.assert_conversion_identical(query)

    def test_trip_duration_hour_of_day_day_of_week(self):
        query = """
select strftime('%H', started_at) as hour_of_day,
       strftime('%w', started_at) as day_of_week,
       COUNT(*) as num_trips
from citibike_partial_demo
group by hour_of_day,
         day_of_week
limit 500"""
        self.assert_conversion_identical(query)

    @unittest.skip
    def test_not_age(self):
        # term parser doesn't handle not between
        query = """
select Age,
       count(*) as count(*)
from data
where Age not between 10 and 20
group by Age
limit 500
"""
        self.assert_conversion_identical(query)

    def test_not_eq(self):
        query = """
select Age,
       count(*) as count(*)
from data
where Age <> 20
group by Age
limit 500
"""
        expected_query = """
select Age,
       count(*) as count(*)
from data
where not Age = 20
group by Age
limit 500
"""
        # NOTE: our parsing changes <> to not =
        self.assert_sql_str_equal(expected_query, self.convert_query(query))

    def test_not_eq_2(self):
        query = """
select Age,
       count(*) as count(*)
from data
where not Age = 20
group by Age
limit 500
"""
        self.assert_conversion_identical(query)

    def test_not_in_departments(self):
        query = """
select Department,
       count(*) as count(*)
from data
where Department not in ('IT', 'Sales')
group by Department
limit 500
"""
        self.assert_conversion_identical(query)

    def test_not_understood_x(self):
        query = """
select YEAR(start_time) as year, COUNT(*) as COUNT_ROWS
from bikeshare_trips
group by year
limit 500
"""
        self.assertEqual(
            """SELECT YEAR(start_time) AS "year",COUNT(*) AS "COUNT_ROWS" FROM "bikeshare_trips" GROUP BY "year" LIMIT 500""",
            self.convert_query(query),
        )
        self.assert_conversion_identical(query)

    def test_join_aliases_multiple(self):
        query = """
select *
from ORDERS o
join LINEITEM l on o.O_ORDERKEY = l.L_ORDERKEY
join CUSTOMER c on o.O_CUSTKEY = c.C_CUSTKEY"""

        expected_query = """
select *
from ORDERS 
join LINEITEM on ORDERS.O_ORDERKEY = LINEITEM.L_ORDERKEY
join CUSTOMER on ORDERS.O_CUSTKEY = CUSTOMER.C_CUSTKEY"""

        compiler = SqliteCompiler(
            DatabaseSchema(
                sql_dialect="Sqlite",
                foreign_keys=[
                    ForeignKey(
                        primary="CUSTOMER.C_CUSTKEY", reference="ORDERS.O_CUSTKEY"
                    ),
                    ForeignKey(
                        primary="ORDERS.O_ORDERKEY", reference="LINEITEM.L_ORDERKEY"
                    ),
                ],
            )
        )
        # we get rid of table alias in parsing
        self.assert_sql_str_equal(
            expected_query,
            compiler.compile(generate_viz_spec(parse_sql(query))).build_str(),
        )

    def test_unparsed_where(self):
        query = """
select O_ORDERKEY,
       O_CUSTKEY,
       O_ORDERSTATUS,
       O_TOTALPRICE,
       O_ORDERDATE,
       O_ORDERPRIORITY,
       O_CLERK,
       O_SHIPPRIORITY,
       O_COMMENT
from ORDERS
where EXTRACT(year
              from O_ORDERDATE) = 2021
              """
        self.assert_conversion_identical(query)

    def test_where_clause_unparsed_2(self):
        query = """
select O_ORDERKEY,
       O_CUSTKEY,
       O_ORDERSTATUS,
       O_TOTALPRICE,
       O_ORDERDATE,
       O_ORDERPRIORITY,
       O_CLERK,
       O_SHIPPRIORITY,
       O_COMMENT
from ORDERS
where YEAR(O_ORDERDATE) = 2021
"""
        self.assert_conversion_identical(query)

    def test_trips_by_station_stoken(self):
        query = """ SELECT count(*) * 100 / (select count(*) from bikeshare_trips) from bikeshare_trips where end_station_name = 'stolen'
"""
        self.assert_conversion_identical(query)

    def test_order_by_add(self):
        query = """
select county_fips_code,
       frequently,
       always
from mask_use_by_county
order by frequently + always desc
limit 10
"""
        self.assert_conversion_identical(query)

    def test_sort_by_term_not_in_select(self):
        query = """
select *
from citibike_partial_demo
order by started_at asc"""
        self.assert_conversion_identical(query)

    def test_average_trip_duration_by_time_of_day(self):
        query = """
SELECT strftime('%H',"started_at") AS "started_at_HOUR_OF_DAY",AVG(strftime('%s',ended_at) - strftime('%s',started_at)) AS "avg_duration" FROM "citibike_partial_demo" GROUP BY "started_at_HOUR_OF_DAY" LIMIT 500
"""

        self.assert_conversion_identical(query)

    def test_where_is_null_check(self):
        query = """ select * from EmployeeData where Country = 'Asia' and Exit_Date is null """
        self.assert_conversion_identical(query)

    def test_complicated_breakdown(self):
        query = """ select Name,
       count(distinct role)
from League_of_Legends_Champion_Stats_13_1
group by Name
having count(distinct role) > 1 """
        self.assert_conversion_identical(query)

    def test_subquery_where(self):
        query = """
SELECT COUNT(*) FROM EmployeeData WHERE Hire_Date < (SELECT Hire_Date FROM EmployeeData WHERE Full_Name = 'easton bailey')
"""
        self.assert_conversion_identical(query)

    def test_league_query(self):
        query = """
        select PlayerName,
       Won / (Won + 1) as WinRate
from LeaguePlayers
order by Won / (Won + 1) desc
limit 10
        """
        self.assert_conversion_identical(query)

    def test_having_query(self):
        query = """
select Player,
       Region,
       avg(Won) as WinRate
from LeaguePlayers
group by Player,
         Region
having count(*) >= 5
order by WinRate desc
limit 10"""

        print(generate_viz_spec(parse_sql(query)))
        self.assert_conversion_identical(query)
