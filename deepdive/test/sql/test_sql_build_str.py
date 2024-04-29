from pypika.terms import Field

import pypika.functions as fn
from deepdive.schema import SqlDialect
from deepdive.sql.parser.sql_tree import SqlTree
from deepdive.test.sql.sql_test_case import SqlTestCase


class TestSqlBuildStr(SqlTestCase):
    def test_backtick_google(self):
        self.assertEquals(
            """SELECT `a` FROM `customers`""",
            SqlTree(
                select_terms=[Field("a")],
                from_term="customers",
                sql_dialect=SqlDialect.GOOGLE_SQL,
            ).build_str(),
        )

    def test_double_quote_snowflake(self):
        self.assertEquals(
            """SELECT a AS b FROM customers""",
            SqlTree(
                select_terms=[Field("a").as_("b")],
                from_term="customers",
                sql_dialect=SqlDialect.SNOWFLAKE_SQL,
            ).build_str(),
        )

    def test_select_count_star_alias(self):
        self.assertEquals(
            """SELECT COUNT(*) AS COUNT(*) FROM customers""",
            SqlTree(
                select_terms=[fn.Count("*").as_("COUNT(*)")],
                from_term="customers",
                sql_dialect=SqlDialect.SNOWFLAKE_SQL,
            ).build_str(),
        )
