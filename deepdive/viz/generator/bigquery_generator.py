import logging

from pypika.terms import Field, Function, ValueWrapper
from pypika.functions import Extract

from deepdive.schema import Binner, DatabaseSchema, XAxis
from deepdive.sql.parser.sql_tree import GroupbyTerm
from deepdive.sql.parser.term_parser import UnparsedField
from deepdive.viz.generator.base_generator import BaseGenerator
from deepdive.viz.processor import VizSpecProcessor
from deepdive.viz.helper import FORMAT_STRING_TO_TIME_UNIT

logger = logging.getLogger(__name__)


class BigQueryGenerator(BaseGenerator):
    def __init__(
        self, db_schema: DatabaseSchema, viz_spec_processor: VizSpecProcessor
    ) -> "BigQueryGenerator":
        super().__init__(db_schema, viz_spec_processor)

    def term_to_x_axis(self, term: GroupbyTerm) -> XAxis:
        x_axis = None

        if isinstance(term, UnparsedField):
            x_axis = XAxis(name=term.name, unparsed=True)
        elif isinstance(term, Field):
            x_axis = XAxis(name=term.name)
        elif isinstance(term, Function) and term.name == "FORMAT_DATE":
            x_axis = self._format_date_to_x_axis(term)
        elif isinstance(term, Function) and term.name == "DATE":
            x_axis = self._date_to_x_axis(term)
        elif isinstance(term, Function) and term.name == "DATE_TRUNC":
            x_axis = self._date_trunc_to_x_axis(term)
        elif isinstance(term, Extract):
            x_axis = self._extract_to_x_axis(term)
        else:
            logger.error("Could not parse term to X Axis: " + term)
            x_axis = XAxis(name=term.get_sql(), unparsed=True)

        if term.alias:
            x_axis.alias = term.alias

        return x_axis

    def _format_date_to_x_axis(self, func: Function) -> XAxis:
        if len(func.args) == 2:
            # of format fn.Function("FORMAT_DATE", "%Y-%m", Field("started_at"))
            return XAxis(
                name=func.args[1].name,
                binner=Binner(
                    binner_type="datetime",
                    time_unit=FORMAT_STRING_TO_TIME_UNIT[func.args[0].value],
                ),
            )

        raise ValueError("Could not parse FORMAT_DATE function: " + func)

    def _date_to_x_axis(self, func: Function) -> XAxis:
        # DATE(created_at) is the same as FORMAT_DATE("%Y-%m-%d", created_at)
        if len(func.args) == 1:
            return XAxis(
                name=func.args[0].name,
                binner=Binner(binner_type="datetime", time_unit="day"),
            )
        raise ValueError("Could not parse date function: " + func)

    def _date_trunc_to_x_axis(self, func: Function) -> XAxis:
        # DATE_TRUNC(created_at, WEEK) truncates the datetime to the start of the week
        if len(func.args) == 2:
            field, date_part = func.args

            # DATE_TRUNC in GoogleSQL is swapped compared to the standard
            # GPT hallucinates often and tends to generate either order, so do a bit of hacking
            if isinstance(date_part, Field):
                field, date_part = date_part, field

            if not isinstance(field, Field) or not isinstance(date_part, ValueWrapper):
                raise ValueError("Could not parse DATE_TRUNC function: " + func)

            return XAxis(
                name=field.name,
                binner=Binner(
                    binner_type="datetime",
                    time_unit=self._parse_date_trunc_date_part(date_part.value),
                ),
            )

        raise ValueError("Could not parse DATE_TRUNC function: " + func)

    def _parse_date_trunc_date_part(self, date_part: str) -> str:
        date_part = date_part.upper()
        if date_part == "WEEK" or date_part == "ISOWEEK":
            return "week"
        elif date_part == "DAY":
            return "day"
        elif date_part == "DAYOFWEEK":
            return "day_of_week"
        elif date_part == "MONTH":
            return "month"
        elif date_part == "YEAR" or date_part == "ISOYEAR":
            return "year"

        raise ValueError("Could not find corresponding date part string: " + date_part)

    def _extract_to_x_axis(self, func: Extract) -> XAxis:
        # EXTRACT(MONTH from started_at) gets month of year term (month is 00-12)
        # https://cloud.google.com/bigquery/docs/reference/standard-sql/date_functions#extract

        if len(func.args) == 1:
            field, date_part = func.field, func.args[0]
            return XAxis(
                name=field.name,
                binner=Binner(
                    binner_type="datetime",
                    time_unit=self._parse_extract_date_part(str(date_part)),
                ),
            )

    def _parse_extract_date_part(self, date_part: str):
        date_part = date_part.upper()
        if date_part == "MONTH":
            return "month_of_year"
        elif date_part == "DAY":
            return "day_of_month"
        elif date_part == "WEEK":
            return "week_of_year"
        elif date_part == "YEAR" or date_part == "ISOYEAR":
            return "year"
        elif date_part == "HOUR":
            return "hour_of_day"

        raise ValueError("Could not find corresponding date part string: " + date_part)
