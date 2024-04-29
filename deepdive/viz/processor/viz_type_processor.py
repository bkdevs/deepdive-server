from typing import Optional
from deepdive.schema import VizSpec, VizType, DatabaseSchema, ColumnSchema, ColumnType
from deepdive.viz.processor.viz_spec_processor import VizSpecProcessor


class VizTypeProcessor(VizSpecProcessor):
    """
    A processor that guesses appropriate visualization types given a generated VizSpec
    """

    def __init__(self, db_schema: DatabaseSchema) -> "VizTypeProcessor":
        self.db_schema = db_schema

    def process(self, viz_spec: VizSpec) -> VizSpec:
        viz_spec.visualization_type = self._guess_viz_type(viz_spec)
        return viz_spec

    def _guess_viz_type(self, viz_spec: VizSpec) -> VizType:
        x_axis, y_axises, breakdowns = (
            viz_spec.x_axis,
            viz_spec.y_axises,
            viz_spec.breakdowns,
        )

        if x_axis:
            x_axis_type = self._get_x_axis_type(viz_spec)
            if (
                x_axis_type == ColumnType.INT
                or x_axis_type == ColumnType.FLOAT
                and len(breakdowns) <= 1
            ):
                return VizType.LINE

        if not x_axis or len(breakdowns) >= 2:
            return VizType.TABLE
        if len(y_axises) > 1 and len(breakdowns) == 1:
            return VizType.BAR
        if len(y_axises) == 1 and len(breakdowns) == 1:
            return VizType.BAR
        if len(breakdowns) == 0:
            x_axis_type = self._get_x_axis_type(viz_spec)
            if x_axis_type == ColumnType.DATE:
                return VizType.LINE
            if x_axis.binner and x_axis.binner.binner_type == "datetime":
                # no explicit datetime field, but we binned it as a date
                return VizType.LINE
            return VizType.BAR

        return VizType.TABLE

    def _get_x_axis_type(self, viz_spec: VizSpec) -> Optional[ColumnSchema]:
        for table in viz_spec.tables:
            table = self.db_schema.get_table(table)
            if not table:
                continue

            column = table.get_column(viz_spec.x_axis.name)
            if column:
                return column.column_type

        return None
