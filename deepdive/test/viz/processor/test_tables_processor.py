import unittest

from deepdive.schema import *
from deepdive.viz.processor.tables_processor import TablesProcessor


class TestTablesProcessor(unittest.TestCase):
    def setUp(self):
        db_schema = DatabaseSchema(
            sql_dialect=SqlDialect.SQLITE,
            tables=[
                TableSchema(
                    name="customers",
                    columns=[
                        ColumnSchema(name="customer_id", column_type=ColumnType.TEXT),
                        ColumnSchema(name="customer_val", column_type=ColumnType.INT),
                    ],
                ),
                TableSchema(
                    name="orders",
                    columns=[
                        ColumnSchema(name="orders_id", column_type=ColumnType.TEXT),
                        ColumnSchema(name="orders_val", column_type=ColumnType.INT),
                    ],
                ),
            ],
        )
        self.processor = TablesProcessor(db_schema)

    def test_no_change_for_columns_complete(self):
        viz_spec = VizSpec(y_axises=[YAxis(name="customer_id")], tables=["customers"])
        self.assertEqual(viz_spec, self.processor.process(viz_spec))

    def test_no_change_for_columns_unknown(self):
        viz_spec = VizSpec(y_axises=[YAxis(name="b"), YAxis(name="c")])
        self.assertEqual(viz_spec, self.processor.process(viz_spec))

    def test_change_for_missing_table(self):
        viz_spec = VizSpec(y_axises=[YAxis(name="customer_id")])
        expected = VizSpec(y_axises=[YAxis(name="customer_id")], tables=["customers"])
        self.assertEqual(expected, self.processor.process(viz_spec))

    def test_change_for_missing_table_2(self):
        viz_spec = VizSpec(
            y_axises=[YAxis(name="customer_id"), YAxis(name="orders_id")],
            tables=["customers"],
        )
        expected = VizSpec(
            y_axises=[YAxis(name="customer_id"), YAxis(name="orders_id")],
            tables=["customers", "orders"],
        )
        self.assertEqual(expected, self.processor.process(viz_spec))

    def test_change_for_missing_table_filter(self):
        viz_spec = VizSpec(
            y_axises=[YAxis(name="customer_id")],
            filters=[
                Filter(name="orders_id", filter_type="comparison", values=["123"])
            ],
            tables=["customers"],
        )
        expected = VizSpec(
            y_axises=[YAxis(name="customer_id")],
            filters=[
                Filter(name="orders_id", filter_type="comparison", values=["123"])
            ],
            tables=["customers", "orders"],
        )
        self.assertEqual(expected, self.processor.process(viz_spec))
