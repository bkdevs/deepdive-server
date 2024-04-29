import logging

from pypika.terms import Field, Function, ValueWrapper
from pypika.functions import Extract

from deepdive.schema import Binner, DatabaseSchema, XAxis
from deepdive.sql.parser.sql_tree import GroupbyTerm
from deepdive.sql.parser.term_parser import UnparsedField
from deepdive.viz.generator.base_generator import BaseGenerator
from deepdive.viz.processor import VizSpecProcessor
from deepdive.viz.compiler.snowflake_compiler import TIME_UNIT_TO_DATE_PART

DATE_PART_TO_TIME_UNIT = {v: k for k, v in TIME_UNIT_TO_DATE_PART.items()}

logger = logging.getLogger(__name__)


class SnowflakeGenerator(BaseGenerator):
    def __init__(
        self, db_schema: DatabaseSchema, viz_spec_processor: VizSpecProcessor
    ) -> "SnowflakeGenerator":
        super().__init__(db_schema, viz_spec_processor)

    def term_to_x_axis(self, term: GroupbyTerm) -> XAxis:
        x_axis = None

        if isinstance(term, UnparsedField):
            x_axis = XAxis(name=term.name, unparsed=True)
        elif isinstance(term, Field):
            x_axis = XAxis(name=term.name)
        elif isinstance(term, Function) and term.name == "DATE":
            x_axis = self._date_to_x_axis(term)
        elif isinstance(term, Function) and term.name == "DATE_TRUNC":
            x_axis = self._date_trunc_to_x_axis(term)
        elif isinstance(term, Function) and term.name == "YEAR":
            x_axis = self._year_to_x_axis(term)
        elif isinstance(term, Extract):
            x_axis = self._extract_to_x_axis(term)
        else:
            logger.error("Could not parse term to X Axis: " + term)
            x_axis = XAxis(name=term.get_sql(), unparsed=True)

        if term.alias:
            x_axis.alias = term.alias

        return x_axis

    def _date_to_x_axis(self, func: Function) -> XAxis:
        # DATE(created_at) is the same as DATE_TRUNC("%Y-%m-%d", created_at)
        if len(func.args) == 1:
            return XAxis(
                name=func.args[0].name,
                binner=Binner(binner_type="datetime", time_unit="day"),
            )
        raise ValueError("Could not parse DATE function: " + func)

    def _year_to_x_axis(self, func: Function) -> XAxis:
        # YEAR(created_at) is the same as DATE_TRUNC("%Y-%m-%d", created_at)
        if len(func.args) == 1:
            return XAxis(
                name=func.args[0].name,
                binner=Binner(binner_type="datetime", time_unit="year"),
            )
        raise ValueError("Could not parse YEAR function: " + func)

    def _date_trunc_to_x_axis(self, func: Function) -> XAxis:
        # DATE_TRUNC(WEEK, created_at) truncates the datetime to the start of the week
        if len(func.args) == 2:
            field, date_part = func.args

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
        date_part = date_part.lower()
        if date_part in DATE_PART_TO_TIME_UNIT:
            return DATE_PART_TO_TIME_UNIT[date_part]

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
        date_part = date_part.lower()
        if date_part == "month":
            return "month_of_year"
        elif date_part == "day":
            return "day_of_month"
        elif date_part == "week":
            return "week_of_year"
        elif date_part == "year" or date_part == "isoyear":
            return "year"
        elif date_part == "hour":
            return "hour_of_day"

        raise ValueError("Could not find corresponding date part string: " + date_part)
