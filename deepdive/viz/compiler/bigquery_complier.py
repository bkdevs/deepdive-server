from typing import Optional
from pypika import functions as fn
from pypika.terms import Field, LiteralValue, Term

from deepdive.schema import DatabaseSchema, XAxis
from deepdive.viz.compiler.base_compiler import BaseCompiler
from deepdive.viz.compiler.helper import column_to_term
from deepdive.sql.parser.sql_tree import WhereTerm
from deepdive.viz.helper import TIME_UNIT_TO_FORMAT_STRING


class BigQueryCompiler(BaseCompiler):
    def __init__(self, db_schema: DatabaseSchema) -> "BigQueryCompiler":
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
                        "FORMAT_DATE",
                        TIME_UNIT_TO_FORMAT_STRING[time_unit],
                        Field(x_axis.name),
                    )
                elif time_unit == "week":
                    term = fn.Function(
                        "DATE_TRUNC",
                        Field(x_axis.name),
                        LiteralValue("WEEK"),  # no quotes
                    )
                elif time_unit == "month_of_year":
                    term = fn.Extract("MONTH", Field(x_axis.name))

            else:
                raise ValueError("Numeric binner currently unsupported!")

        if x_axis.alias:
            term = term.as_(x_axis.alias)
        return term

    def x_axis_to_where(
        self, x_axis: XAxis, x_axis_alias_or_term: Term
    ) -> Optional[WhereTerm]:
        """
        BigQuery does not support using aliases in where clauses:
        https://cloud.google.com/bigquery/docs/reference/standard-sql/query-syntax#where_clause

        ```
        The WHERE clause only references columns available via the FROM clause; it cannot reference SELECT list aliases.
        ```

        So we explicitly convert the x-axis to a term here and set its alias to None
        """
        if not x_axis.domain:
            return None

        x_axis_term = self.x_axis_to_term(x_axis)
        x_axis_term.alias = None

        domainMin, domainMax = x_axis.domain

        if domainMin and domainMax:
            return x_axis_term[domainMin:domainMax]
        elif domainMin:
            return x_axis_term >= domainMin
        elif domainMax:
            return x_axis_term <= domainMax

        return None
