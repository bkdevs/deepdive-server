import logging
from typing import Optional

from pypika.terms import Field, Function

from deepdive.schema import Binner, DatabaseSchema, XAxis
from deepdive.sql.parser.term_parser import UnparsedField
from deepdive.sql.parser.sql_tree import GroupbyTerm
from deepdive.viz.helper import FORMAT_STRING_TO_TIME_UNIT
from deepdive.viz.generator.base_generator import BaseGenerator
from deepdive.viz.processor import VizSpecProcessor

logger = logging.getLogger(__name__)


class SqliteGenerator(BaseGenerator):
    def __init__(
        self, db_schema: DatabaseSchema, viz_spec_processor: VizSpecProcessor
    ) -> "SqliteGenerator":
        super().__init__(db_schema, viz_spec_processor)

    def term_to_x_axis(self, term: GroupbyTerm) -> XAxis:
        x_axis = None

        if isinstance(term, UnparsedField):
            x_axis = XAxis(name=term.name, unparsed=True)
        elif isinstance(term, Field):
            x_axis = XAxis(name=term.name)
        elif isinstance(term, Function) and (
            term.name == "strftime" or term.name == "STRFTIME"
        ):
            x_axis = self._strftime_to_x_axis(term)
        elif isinstance(term, Function) and (
            term.name == "DATE" or term.name == "date"
        ):
            x_axis = self._date_to_x_axis(term)
        else:
            logger.error("Could not parse term to X Axis: " + term)
            x_axis = XAxis(name=term.get_sql(), unparsed=True)

        if term.alias:
            x_axis.alias = term.alias

        return x_axis

    def _strftime_to_x_axis(self, func: Function) -> XAxis:
        if len(func.args) == 2:
            # of format fn.Function("strftime", "%Y-%m", Field("started_at"))
            return XAxis(
                name=func.args[1].name,
                binner=Binner(
                    binner_type="datetime",
                    time_unit=FORMAT_STRING_TO_TIME_UNIT[func.args[0].value],
                ),
            )
        elif len(func.args) == 4:
            # sqlite get start of week syntax
            # e.g, fn.Function("strftime", "Y-%m-%d", Field("started_at"), "weekday 0", "-6 days")
            if (
                "%Y-%m-%d" == func.args[0]
                and "weekday 0" == func.args[2]
                and "-6 days" == func.args[3]
            ):
                return XAxis(
                    name=func.args[1].name,
                    binner=Binner(binner_type="datetime", time_unit="week"),
                )

        raise ValueError("Could not parse strftime function: " + func)

    def _date_to_x_axis(self, func: Function) -> XAxis:
        # date(created_at) is the same as strftime("%Y-%m-%d")
        if len(func.args) == 1:
            return XAxis(
                name=func.args[0].name,
                binner=Binner(binner_type="datetime", time_unit="day"),
            )
        raise ValueError("Could not parse date function: " + func)
