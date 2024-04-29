import logging
from typing import Dict

from pandas import DataFrame as PandasDataFrame
from pandas.api.types import is_object_dtype
from snowflake.snowpark import DataFrame as SnowparkDataFrame
from snowflake.snowpark.types import (
    DataType,
    DateType,
    StringType,
    TimestampType,
    _FractionalType,
    _IntegralType,
)

logger = logging.getLogger(__name__)


def _snowpark_type_to_dtype(snowpark_type: DataType) -> str:
    if isinstance(snowpark_type, _IntegralType):
        return "int"
    elif isinstance(snowpark_type, _FractionalType):
        return "float"
    elif isinstance(snowpark_type, DateType) or isinstance(
        snowpark_type, TimestampType
    ):
        return "datetime64"
    elif isinstance(snowpark_type, StringType):
        return "str"
    else:
        logger.error(f"Unsupported snowpark type: {snowpark_type}!")
        return "str"


def _snowpark_types(snowpark_df: SnowparkDataFrame) -> Dict[str, str]:
    types = {}
    for field in snowpark_df.schema.fields:
        types[field.name] = _snowpark_type_to_dtype(field.datatype)
    return types


def infer_missing_dtypes(
    snowpark_df: SnowparkDataFrame, pandas_df: PandasDataFrame
) -> PandasDataFrame:
    """
    A Snowpark DataFrame sometimes contains dtypes that do not translate to pandas DataFrames

    This leads to a loss of type information, e.g:
      >>> snowflake_df.dtypes
      [('MONTH', 'date'), ('REVENUE', 'decimal(37,4)')]
      >>> pandas_df.dtypes
      MONTH      object
      REVENUE    object
      dtype: object

    This function uses the Snowpark DF to infer missing dtypes in the pandas dataframe
    """

    snowpark_types = _snowpark_types(snowpark_df)

    types = {}
    for column in pandas_df:
        column_type = pandas_df.dtypes[column]
        if is_object_dtype(column_type):
            types[column] = snowpark_types[column]
        else:
            types[column] = column_type

    return pandas_df.astype(types)
