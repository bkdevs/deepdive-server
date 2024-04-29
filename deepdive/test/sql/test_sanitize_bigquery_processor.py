import unittest

from deepdive.sql.processor.sanitize_bigquery_processor import SanitizeBigQueryProcessor


class TestSanitizeBigQueryProcessor(unittest.TestCase):
    def setUp(self):
        self.processor = SanitizeBigQueryProcessor()

    def test_does_nothing_for_normal_table(self):
        query = "SELECT * FROM customers"
        self.assertEqual("SELECT * FROM customers", self.processor.process(query))

    def test_sanitizes_digit_table(self):
        query = "SELECT * FROM 123customers"
        self.assertEqual("SELECT * FROM `123customers`", self.processor.process(query))

    def test_sanitizes_keywords_table(self):
        query = "SELECT * FROM full"
        self.assertEqual("SELECT * FROM `full`", self.processor.process(query))

    def test_sanitizes_keywords_column(self):
        query = "SELECT by FROM table1"
        self.assertEqual("SELECT `by` FROM table1", self.processor.process(query))

    def test_sanitizes_keywords_a_lot(self):
        query = "SELECT by, to FROM full"
        self.assertEqual("SELECT `by`, `to` FROM `full`", self.processor.process(query))

    @unittest.skip
    def test_works_for_multiple_tables(self):
        # TODO: Parser doesn't parse the second table if it starts with a digit
        # interestingly, it does parse it if doesn't
        query = "SELECT customerID from customers WHERE ORDER_ID in (SELECT * FROM 123orders WHERE O_DISCOUNT > 0)"
        self.assertEqual(
            "SELECT customerID from customers WHERE ORDER_ID in (SELECT * FROM `123orders` WHERE O_DISCOUNT > 0)",
            self.processor.process(query),
        )
