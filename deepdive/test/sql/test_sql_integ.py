import unittest

from deepdive.sql.parser import parse_sql
from deepdive.test.sql.sql_test_case import SqlTestCase


class TestSqlInteg(SqlTestCase):
    """
    Tests that we can translate to a SqlTree and back, an end-to-end test of our parsing functionality
    """

    def assert_conversion_identical(self, query):
        self.assert_sql_str_equal(query, parse_sql(query).build_str())

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
        self.assert_conversion_identical(query)

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
join zipcodes on JC_202307_citibike_tripdata.start_lat = zipcodes.lat
and JC_202307_citibike_tripdata.start_lng = zipcodes.lng"""

        self.assert_conversion_identical(query)

    def test_daily_trips(self):
        query = """
select DATE(started_at) as date,
       COUNT(*) as num_trips
from JC_202307_citibike_tripdata
group by DATE(started_at)
limit 500"""
        self.assert_conversion_identical(query)

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
        self.assert_conversion_identical(query)

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
from data as Table_Data
where Department = 'IT'
  or Department = 'Business'
limit 500"""
        self.assert_conversion_identical(query)

    def test_table_alias_as(self):
        query = """
select *
from data as Table_Data
where Department = 'IT'
  or Department = 'Business'
limit 500"""
        self.assert_conversion_identical(query)

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

    @unittest.skip
    def test_not_age(self):
        query = """
select Age,
       count(*) as count(*)
from data
where Age not between 10 and 20
group by Age
limit 500
"""
        self.assert_conversion_identical(query)

    def test_not_in_departments(self):
        query = """
select Department,
       count(*) as count(*)
from data
where Department not in ('IT',
                     'Sales')
group by Department
limit 500
"""
        self.assert_conversion_identical(query)
