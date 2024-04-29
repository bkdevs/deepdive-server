import unittest

from pypika import Table
from pypika.terms import Term

from deepdive.schema import DatabaseSchema
from deepdive.sql.parser.sql_tree import SqlTree
from deepdive.sql.parser.util import normalize_query
from deepdive.viz.compiler.sqlite_compiler import SqliteCompiler
from deepdive.viz.generator.sqlite_generator import SqliteGenerator
from deepdive.viz.processor import NoopProcessor

DB_SCHEMA = DatabaseSchema(sql_dialect="Sqlite", tables=[])
COMPILER = SqliteCompiler(DB_SCHEMA)
GENERATOR = SqliteGenerator(DB_SCHEMA, NoopProcessor())


def compile_viz_spec(viz_spec):
    return COMPILER.compile(viz_spec)


def generate_viz_spec(sql_tree):
    return GENERATOR.generate(sql_tree)


class SqlTestCase(unittest.TestCase):
    def assert_sql_str_equal(self, original_expression, converted_expression):
        self.assertEqual(
            normalize_query(original_expression), normalize_query(converted_expression)
        )

    def assertTermEquals(self, expected: Term, actual: Term):
        # terms are hard to compare since they override __eq__
        self.assertFalse(expected is None and actual is not None)
        self.assertFalse(expected is not None and actual is None)
        if expected and actual:
            self.assertEqual(type(actual), type(expected))
            self.assertIsInstance(actual, type(expected))
            self.assertEquals(expected, actual)

            if isinstance(expected, Term):
                self.assertEquals(expected.alias, actual.alias)
                self.assert_sql_str_equal(expected.get_sql(), actual.get_sql())

    def assertTreeEquals(self, expected: SqlTree, actual: SqlTree):
        self.assertEquals(expected, actual)

        for i, select_term in enumerate(expected.select_terms):
            self.assertTermEquals(select_term, actual.select_terms[i])

        if expected.groupby_terms:
            for i, groupby_term in enumerate(expected.groupby_terms):
                self.assertTermEquals(groupby_term, actual.groupby_terms[i])

        if expected.joinon_terms:
            for i, joinon_term in enumerate(expected.joinon_terms):
                self.assertTermEquals(joinon_term, actual.joinon_terms[i])

        if expected.orderby_term:
            self.assertTermEquals(expected.orderby_term[0], actual.orderby_term[0])
            self.assertEquals(expected.orderby_term[1], actual.orderby_term[1])

        if isinstance(expected.from_term, Table):
            self.assert_sql_str_equal(
                expected.from_term.get_sql(), actual.from_term.get_sql()
            )

        self.assertTermEquals(expected.where_term, actual.where_term)
