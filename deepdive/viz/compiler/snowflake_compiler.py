from pypika import functions as fn
from pypika.terms import Field, LiteralValue, Term

from deepdive.schema import DatabaseSchema, XAxis
from deepdive.viz.compiler.base_compiler import BaseCompiler
from deepdive.viz.compiler.helper import column_to_term

# https://docs.snowflake.com/en/sql-reference/functions-conversion#label-date-time-format-conversion
TIME_UNIT_TO_DATE_FORMAT = {
    "hour_of_day": "HH24",
    "day_of_month": "DD",
}

# https://docs.snowflake.com/en/sql-reference/functions-date-time#label-supported-date-time-parts
TIME_UNIT_TO_DATE_PART = {
    "day": "day",
    "week": "week",
    "month": "month",
    "year": "year",
    "day_of_week": "dayofweek",
    "day_of_year": "dayofyear",
    "hour": "hour",
    "minute": "minute",
    "second": "second",
}


class SnowflakeCompiler(BaseCompiler):
    def __init__(self, db_schema: DatabaseSchema) -> "SnowflakeCompiler":
        super().__init__(db_schema)

    def x_axis_to_term(self, x_axis: XAxis) -> Term:
        term = column_to_term(x_axis.name)
        if x_axis.unparsed:
            term = LiteralValue(x_axis.name)
        if x_axis.binner:
            if x_axis.binner.binner_type == "datetime":
                time_unit = x_axis.binner.time_unit
                if time_unit in TIME_UNIT_TO_DATE_PART:
                    term = fn.Function(
                        "DATE_TRUNC",
                        LiteralValue(TIME_UNIT_TO_DATE_PART[time_unit]),
                        Field(x_axis.name),
                    )
                elif time_unit in TIME_UNIT_TO_DATE_FORMAT:
                    term = fn.Function(
                        "TO_VARCHAR",
                        Field(x_axis.name),
                        TIME_UNIT_TO_DATE_FORMAT[time_unit],
                    )
                elif time_unit == "month_of_year":
                    term = fn.Extract("MONTH", Field(x_axis.name))
            else:
                raise ValueError("Numeric binner currently unsupported!")

        if x_axis.alias:
            term = term.as_(x_axis.alias)
        return term
