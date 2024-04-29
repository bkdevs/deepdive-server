from deepdive.sql.processor.limit_processor import LimitProcessor
from deepdive.sql.parser.sql_tree import SqlTree
from deepdive.test.sql.sql_test_case import SqlTestCase

LIMIT = 1000


class TestLimitProcessor(SqlTestCase):
    def setUp(self):
        self.processor = LimitProcessor(LIMIT)

    def test_adds_limit(self):
        tree = SqlTree(select_terms=["*"], from_term="customers")
        self.assertTreeEquals(
            SqlTree(select_terms=["*"], from_term="customers", limit_term=1000),
            self.processor.process(tree),
        )

    def test_doesnt_add_limit(self):
        tree = SqlTree(select_terms=["*"], from_term="customers", limit_term=10)
        self.assertTreeEquals(tree, self.processor.process(tree))
