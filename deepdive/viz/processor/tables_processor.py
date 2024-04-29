from typing import List, Set, Optional

from deepdive.viz.processor.viz_spec_processor import VizSpecProcessor
from deepdive.schema import VizSpec, DatabaseSchema


class TablesProcessor(VizSpecProcessor):
    """
    Based on the axes present in the visualization spec, we determine the set of relevant tables that should be included

    This doesn't account for duplicate column names as of now
    """

    def __init__(self, db_schema: DatabaseSchema) -> "TablesProcessor":
        self.db_schema = db_schema

    def process(self, viz_spec: VizSpec) -> VizSpec:
        if not viz_spec:
            return None

        tables_columns = self._get_table_columns(viz_spec.tables)
        all_columns = viz_spec.get_all_columns() + viz_spec.get_filter_columns()
        for column in all_columns:
            if column not in tables_columns:
                table = self._find_table(column)
                if table:
                    viz_spec.tables.append(table)
                    tables_columns = self._get_table_columns(viz_spec.tables)

        return viz_spec

    def _get_table_columns(self, tables: List[str]) -> Set[str]:
        all_columns = set()
        for table in tables:
            all_columns.update(
                [column.name for column in self.db_schema.get_table(table).columns]
            )
        return all_columns

    def _find_table(self, column_to_find: str) -> Optional[str]:
        for table in self.db_schema.tables:
            for column in table.columns:
                if column.name == column_to_find:
                    return table.name

        return None
