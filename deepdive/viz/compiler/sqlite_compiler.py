from typing import Optional
from pypika import functions as fn
from pypika.terms import Field, Term, LiteralValue

from deepdive.schema import DatabaseSchema, XAxis, VizSpec
from deepdive.viz.compiler.base_compiler import BaseCompiler
from deepdive.viz.compiler.helper import column_to_term
from deepdive.viz.helper import TIME_UNIT_TO_FORMAT_STRING


class SqliteCompiler(BaseCompiler):
    def __init__(self, db_schema: DatabaseSchema) -> "SqliteCompiler":
        super().__init__(db_schema)

    def x_axis_to_term(self, x_axis: XAxis) -> Term:
        term = column_to_term(x_axis.name)
        if x_axis.unparsed:
            term = LiteralValue(x_axis.name)
        if x_axis.binner:
            if x_axis.binner.binner_type == "datetime":
                time_unit = x_axis.binner.time_unit
                if time_unit in TIME_UNIT_TO_FORMAT_STRING:
                    term = fn.Function(
                        "strftime",
                        TIME_UNIT_TO_FORMAT_STRING[time_unit],
                        Field(x_axis.name),
                    )
                elif time_unit == "week":
                    term = fn.Function(
                        "strftime",
                        "%Y-%m-%d",
                        Field(x_axis.name),
                        "weekday 0",
                        "-6 days",
                    )
            else:
                raise ValueError("Numeric binner currently unsupported!")

        if x_axis.alias:
            term = term.as_(x_axis.alias)
        return term
