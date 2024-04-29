import unittest
from pypika.terms import Function, Field
from pypika.functions import Count, AggregateFunction
from deepdive.sql.parser.term_parser import parse_term
from deepdive.test.sql.sql_test_case import SqlTestCase


class TestTermParser(SqlTestCase):
    """
    Tests that we can translate to an PyPika Term, usually one of
      - Criterion: e.g, "a = 10"
      - Function: e.g, "COUNT(*)"
    """

    def assert_conversion_identical(self, expression):
        self.assert_sql_str_equal(expression, str(parse_term(expression)))

    def test_simple_eq(self):
        expression = "a = 10"
        self.assert_conversion_identical(expression)

    def test_simple_not_eq(self):
        expression = "a <> 10"
        self.assert_conversion_identical(expression)

    def test_simple_not_eq_2(self):
        expression = "not a = 10"
        self.assert_conversion_identical(expression)

    def test_simple_gt(self):
        expression = "a > 10"
        self.assert_conversion_identical(expression)

    def test_simple_lt(self):
        expression = "a < 10"
        self.assert_conversion_identical(expression)

    def test_simple_in(self):
        expression = "a in ('foo', 'bar')"
        self.assert_conversion_identical(expression)

    def test_simple_not_in(self):
        expression = "not a in ('foo', 'bar')"
        self.assert_conversion_identical(expression)

    def test_simple_and(self):
        expression = "a > 10 and a < 20"
        self.assert_conversion_identical(expression)

    @unittest.skip
    def test_simple_not_between(self):
        # term parser doesn't handle not between currently
        expression = "a NOT between 10 and 20"
        self.assert_conversion_identical(expression)

    def test_simple_not_and(self):
        expression = "not a > 10 and a < 20"
        self.assert_sql_str_equal(
            "not (a > 10 and a < 20)", str(parse_term(expression))
        )

    def test_simple_add(self):
        expression = "a + 10"
        self.assert_conversion_identical(expression)

    def test_where_greater_than(self):
        expression = """started_at >= '2023-07-16 00:00:00' and ended_at <= '2023-07-16 23:59:59'"""
        self.assert_conversion_identical(expression)

    def test_where_or(self):
        expression = """Department = 'IT' or Department = 'Business'"""
        self.assert_conversion_identical(expression)

    def test_where_and(self):
        expression = """Department = 'IT' and Department = 'Business'"""
        self.assert_conversion_identical(expression)

    def test_where_like(self):
        expression = """start_station_name LIKE '%Manhattan'"""
        self.assert_conversion_identical(expression)

    def test_where_like_or(self):
        expression = """start_station_name LIKE '%Manhattan' or end_station_name LIKE '%Manhattan'"""
        self.assert_conversion_identical(expression)

    def test_simple_eq_dot(self):
        expression = "customers.a = 10"
        self.assert_conversion_identical(expression)

    def test_count_star(self):
        expression = "COUNT(*)"
        func = parse_term(expression)

        self.assertIsInstance(func, Count)
        self.assert_conversion_identical(expression)

    def test_count_column(self):
        expression = "COUNT(customerId)"
        func = parse_term(expression)

        self.assertIsInstance(func, Count)
        self.assert_conversion_identical(expression)

    def test_avg_func(self):
        expression = "AVG(Annual_Salary)"
        func = parse_term(expression)

        self.assertIsInstance(func, Function)
        self.assertEquals("AVG", func.name)
        self.assertEquals("Annual_Salary", func.args[0].get_sql())
        self.assert_conversion_identical(expression)

    def test_removes_single_quotes(self):
        expression = "'single_quote'"
        field = parse_term(expression)

        self.assertIsInstance(field, Field)
        self.assertEquals("single_quote", field.name)

    def test_removes_double_quotes(self):
        expression = '"single_quote"'
        field = parse_term(expression)

        self.assertIsInstance(field, Field)
        self.assertEquals("single_quote", field.name)

    def test_removes_back_ticks(self):
        expression = "`single_quote`"
        field = parse_term(expression)

        self.assertIsInstance(field, Field)
        self.assertEquals("single_quote", field.name)

    def test_avg_func_alias_backticks(self):
        expression = "AVG(Annual_Salary) as `AVG(Annual_Salary)`"
        func = parse_term(expression)

        self.assertIsInstance(func, Function)
        self.assertEquals("AVG", func.name)
        self.assertEquals("AVG(Annual_Salary)", func.alias)
        self.assertEquals("Annual_Salary", func.args[0].get_sql())

    def test_avg_func_backticks(self):
        expression = "AVG(`Annual_Salary`)"
        func = parse_term(expression)

        self.assertIsInstance(func, Function)
        self.assertEquals("AVG", func.name)
        self.assertEquals("Annual_Salary", func.args[0].get_sql())
        self.assert_conversion_identical(expression)

    def test_parse_where_hard(self):
        expression = "YEAR(O_ORDERDATE) = 2021"
        func = parse_term(expression)
