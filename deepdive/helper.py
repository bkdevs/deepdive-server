from typing import Dict, List, Optional

import pandas as pd
from pandas import DataFrame

from deepdive.schema import ColumnType, DatabaseSchema


def _column_islike_id(column: str):
    column = column.lower()
    return "id" in column or "key" in column


def _column_in_primary_keys(column: str, primary_keys: Optional[List[str]]):
    if not primary_keys:
        return False
    return any([column in primary_key for primary_key in primary_keys])


def _parse_ids(df: DataFrame, db_schema: DatabaseSchema) -> List[str]:
    ids = []
    for column in df:
        if db_schema and _column_in_primary_keys(column, db_schema.primary_keys):
            ids.append(column)
        elif _column_islike_id(column):
            ids.append(column)
    return ids


def _is_datetime_string(row: object) -> bool:
    return isinstance(row, str) and not pd.isnull(pd.to_datetime(row, errors="coerce"))


def _infer_date_columns(df: DataFrame) -> List[str]:
    date_columns = _get_numpy_column_type(df, "datetime")
    for column in _get_numpy_column_type(df, "object"):
        if all(df[column].map(_is_datetime_string)):
            date_columns.append(column)

    return date_columns


def _get_numpy_column_type(df: DataFrame, numpy_column_type: str) -> List[str]:
    return df.select_dtypes(numpy_column_type).columns.values.tolist()


def get_column_types(
    df: DataFrame, db_schema: Optional[DatabaseSchema] = None
) -> Dict[ColumnType, List[str]]:
    ids = _parse_ids(df, db_schema)
    df = df[df.columns.difference(ids)]

    date_columns = _infer_date_columns(df)
    df = df[df.columns.difference(date_columns)]

    return {
        ColumnType.ID: ids,
        ColumnType.TEXT: _get_numpy_column_type(df, "object"),
        ColumnType.BOOLEAN: _get_numpy_column_type(df, "bool"),
        ColumnType.INT: _get_numpy_column_type(df, "integer"),
        ColumnType.FLOAT: _get_numpy_column_type(df, "floating"),
        ColumnType.DATE: date_columns,
    }


def inverse_column_types(
    column_types: Dict[ColumnType, List[str]]
) -> Dict[str, ColumnType]:
    result = {}
    for column_type, columns in column_types.items():
        for column in columns:
            result[column] = column_type
    return result


def get_column_to_types(
    df: DataFrame, db_schema: Optional[DatabaseSchema] = None
) -> Dict[str, ColumnType]:
    return inverse_column_types(get_column_types(df, db_schema))
