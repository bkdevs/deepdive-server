from enum import Enum
from typing import Dict, List, Literal, Optional, Tuple, Union

from pydantic import BaseModel, Field, model_validator


class ColumnType(str, Enum):
    ID = "id"
    TEXT = "text"
    INT = "int"
    FLOAT = "float"
    BOOLEAN = "boolean"
    DATE = "date"
    TIME = "time"
    RECORD = "record"


class ColumnSchema(BaseModel):
    name: str
    column_type: ColumnType
    comment: str = None


class ForeignKey(BaseModel):
    primary: str
    reference: str


class TableSchema(BaseModel):
    name: str
    columns: List[ColumnSchema]

    def get_column(self, column_name: str) -> Optional[ColumnSchema]:
        for column in self.columns:
            if column.name == column_name:
                return column
        return None

    def __lt__(self, other):
        return self.name < other.name


class TablePreview(BaseModel):
    table_schema: TableSchema
    sample_data: str


class SqlDialect(str, Enum):
    SQLITE = "Sqlite"
    SNOWFLAKE_SQL = "Snowflake"
    MY_SQL = "MySQL"
    GOOGLE_SQL = "GoogleSQL"


class DatabaseSchema(BaseModel):
    tables: List[TableSchema] = []
    primary_keys: Optional[List[str]] = []
    foreign_keys: Optional[List[ForeignKey]] = []
    sql_dialect: SqlDialect

    def get_table(self, table_name: str) -> Optional[TableSchema]:
        for table in self.tables:
            if table.name == table_name:
                return table
        return None


class VizType(str, Enum):
    BAR = "bar"
    LINE = "line"
    AREA = "area"
    PIE = "pie"
    TABLE = "table"
    SCATTER = "scatter"


class Binner(BaseModel):
    binner_type: Literal["datetime", "numeric"]
    time_unit: Optional[
        Literal[
            "second",
            "minute",
            "hour",
            "hour_of_day",
            "day",
            "day_of_week",
            "day_of_month",
            "week",
            "week_of_year",
            "week_of_year_long",
            "month",
            "month_of_year",
            "year",
        ]
    ]
    scale: Optional[int] = Field(None, ge=0, le=100)

    @model_validator(mode="after")
    def check_binner_fields_for_type(self) -> "Binner":
        if self.binner_type == "datetime" and not self.time_unit:
            raise ValueError("datetime binners must have time_unit specified")
        if self.binner_type == "numeric" and not self.scale:
            raise ValueError("numeric binners must have scale specified")
        return self


DomainLimit = Union[int, float, str]
Domain = Tuple[Optional[DomainLimit], Optional[DomainLimit]]


class XAxis(BaseModel):
    name: str
    alias: Optional[str] = None
    domain: Optional[Domain] = None
    binner: Optional[Binner] = None
    unparsed: Optional[bool] = False


AggregationFunctions = Literal["COUNT", "SUM", "AVG", "MIN", "MAX"]


class YAxis(BaseModel):
    name: str
    alias: Optional[str] = None
    aggregation: Optional[AggregationFunctions] = None
    unparsed: Optional[bool] = False


class SortBy(BaseModel):
    name: str
    direction: Optional[Literal["asc", "desc"]] = "asc"
    unparsed: Optional[bool] = False


class Filter(BaseModel):
    name: str
    filter_type: Literal["comparison", "numeric", "like", "complex"]
    expression: Optional[str] = None
    subfilters: Optional[Tuple["Filter", "Filter"]] = None
    domain: Optional[Domain] = None
    values: Optional[List[Union[int, float, str]]] = None
    negate: bool = False

    @model_validator(mode="after")
    def check_filter_fields_for_type(self) -> "Filter":
        if self.filter_type == "numeric" and not self.domain:
            raise ValueError("Numeric filters must specify domain")
        if self.filter_type == "comparison" and not self.values:
            raise ValueError("Comparison filters must specify values")
        return self


class Breakdown(BaseModel):
    name: str
    alias: Optional[str] = None
    unparsed: Optional[bool] = False


class VizSpecError(Exception):
    ErrorType = Literal[
        "no_duplicate_axes", "aggregation_not_specified", "sort_by_not_found"
    ]

    def __init__(self, error_type: ErrorType, message: str):
        self.error_type = error_type
        super().__init__(message)


class VizSpecParams(BaseModel):
    """
    An un-validated version of VizSpecParams _only_ to be used in request serialization
    """

    visualization_type: Optional[VizType] = VizType.BAR
    x_axis: Optional[XAxis] = None
    y_axises: List[YAxis] = []
    breakdowns: List[Breakdown] = []
    filters: List[Filter] = []
    tables: Optional[List[str]] = []
    limit: Optional[int] = None
    sort_by: Optional[SortBy] = None


class VizSpec(VizSpecParams):
    @model_validator(mode="after")
    def check_no_duplicate_axes(self) -> "VizSpec":
        all_columns = self.get_all_columns()
        if len(all_columns) != len(set(all_columns)):
            raise VizSpecError(
                error_type="no_duplicate_axes",
                message="Duplicate column name in x_axis, y_axises, and breakdown!",
            )
        return self

    # @model_validator(mode="after")
    # def check_y_axises(self) -> "VizSpec":
    #     if (self.x_axis or len(self.breakdowns) > 0) and self.y_axises:
    #         if any(
    #             y_axis.aggregation is None and not y_axis.unparsed
    #             for y_axis in self.y_axises
    #         ):
    #             raise VizSpecError(
    #                 error_type="aggregation_not_specified",
    #                 message="Y Axis aggregations MUST be specified if an x_axis or a breakdown is specified",
    #             )
    #     return self

    @model_validator(mode="after")
    def check_sort_by(self) -> "VizSpec":
        if self._has_star():
            return self

        if (
            self.sort_by
            and not self.sort_by.unparsed
            and self.sort_by.name not in self.get_all_columns()
        ):
            raise VizSpecError(
                error_type="sort_by_not_found",
                message="Sort by MUST be specified in one of x_axis, y_axises, or breakdown",
            )
        return self

    @model_validator(mode="after")
    def check_no_extra_columns_if_star(self) -> "VizSpec":
        if self._has_star() and len(self.y_axises) > 1:
            raise VizSpecError(
                error_type="extra_column_with_star",
                message="Extra columns should NOT be specified when a star is present",
            )
        return self

    def _has_star(self):
        return any(
            y_axis.name == "*" and y_axis.aggregation is None
            for y_axis in self.y_axises
        )

    def get_all_columns(self) -> List[str]:
        all_columns = []
        if self.x_axis and not self.x_axis.unparsed:
            all_columns.append(self.x_axis.name)
        if self.y_axises:
            all_columns.extend(
                list(
                    set(y_axis.name for y_axis in self.y_axises if not y_axis.unparsed)
                )
            )
        if self.breakdowns:
            all_columns.extend(breakdown.name for breakdown in self.breakdowns)
        return all_columns

    def get_filter_columns(self) -> List[str]:
        filter_columns = []
        for viz_filter in self.filters:
            if viz_filter.filter_type == "complex":
                continue
            filter_columns.append(viz_filter.name)
        return filter_columns


class TableConfig(BaseModel):
    name: str
    excel_params: Optional[Dict] = {}
