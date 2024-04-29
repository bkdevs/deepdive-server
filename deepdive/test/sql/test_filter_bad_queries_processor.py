from deepdive.sql.processor.filter_bad_queries_processor import (
    FilterBadQueriesProcessor,
)
from deepdive.schema import *
from deepdive.sql.parser.sql_tree import SqlTree
from deepdive.test.sql.sql_test_case import SqlTestCase
from pypika import Table


class TestFilterBadQueriesProcessor(SqlTestCase):
    def setUp(self):
        self.processor = FilterBadQueriesProcessor(
            DatabaseSchema(
                sql_dialect=SqlDialect.SQLITE,
                tables=[TableSchema(name="EmployeeData", columns=[])],
            )
        )

    def test_doesnt_filter_valid_table(self):
        tree = SqlTree(select_terms=["*"], from_term="EmployeeData")
        self.assertTreeEquals(tree, self.processor.process(tree))

    def test_doesnt_filter_valid_table_2(self):
        tree = SqlTree(
            select_terms=["*"], from_term=Table("EmployeeData").as_("employee")
        )
        self.assertTreeEquals(tree, self.processor.process(tree))

    def test_does_filter_invalid_table(self):
        tree = SqlTree(select_terms=["*"], from_term="EmployeeData2")
        self.assertIsNone(self.processor.process(tree))

    def test_does_filter_invalid_table_2(self):
        tree = SqlTree(
            select_terms=["*"], from_term=Table("EmployeeData2").as_("empl2")
        )
        self.assertIsNone(self.processor.process(tree))

    def test_does_filter_no_table(self):
        tree = SqlTree(select_terms=["*"])
        self.assertIsNone(self.processor.process(tree))
