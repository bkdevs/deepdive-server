import io
from pathlib import Path
import re
from typing import Dict, List, Tuple

import pandas as pd

from deepdive.database.sqlite_helper import (
    SQLITE_KEYWORD_SUBSTITUTES,
    SQLITE_KEYWORDS,
)
from deepdive.helper import get_column_to_types
from deepdive.schema import ColumnSchema, DatabaseSchema, TableConfig, TableSchema

NUM_SAMPLE_ROWS = 10


def create_table_schema(table_name: str, data: pd.DataFrame) -> TableSchema:
    columns = process_columns(data)
    return TableSchema(
        name=sanitize_table_name(table_name),
        columns=columns,
    )


def process_columns(data: pd.DataFrame) -> List[ColumnSchema]:
    column_schemas = []
    column_types = get_column_to_types(data)
    for column_name in data:
        column_type = column_types[column_name]
        column_schemas.append(
            ColumnSchema(
                name=sanitize_column_name(column_name),
                column_type=column_type,
            )
        )

    return column_schemas


def sanitize_column_name(column_name: str) -> str:
    column_name = column_name.replace(" ", "_")
    column_name = column_name.replace("'", "_")
    if "%" in column_name:
        return column_name.replace("%", "Percentage")
    column_name = re.sub(r"[^a-zA-Z0-9'_]+", "", column_name)
    if column_name.lower() in SQLITE_KEYWORDS:
        return SQLITE_KEYWORD_SUBSTITUTES.get(column_name, "C_" + column_name)
    if column_name and column_name[0].isdigit():
        return f"'{column_name}'"
    return column_name


def validate_column_name(column_name: str):
    if not column_name:
        raise ValueError("Column name cannot be empty!")
    if column_name.lower() in SQLITE_KEYWORDS:
        raise ValueError(
            f"{column_name} is a SQL keyword and cannot be used as a column name! Please rename"
        )
    if column_name[0].isdigit():
        raise ValueError(
            f"{column_name} starts with a number and cannot be used as a column name! Please rename"
        )
    if "%" in column_name:
        raise ValueError("Column name cannot include '%'")


def sanitize_table_name(table_name: str) -> str:
    table_name = table_name.replace(" ", "_")
    if "%" in table_name:
        return table_name.replace("%", "Percentage")
    table_name = re.sub(r"[^a-zA-Z0-9'_]+", "", table_name)
    if table_name.lower() in SQLITE_KEYWORDS:
        return SQLITE_KEYWORD_SUBSTITUTES.get(table_name, "Table" + table_name)
    if table_name and table_name[0].isdigit():
        return f"Table{table_name}"
    return table_name


def sanitize_database_schema(db_schema_string: str) -> DatabaseSchema:
    if not db_schema_string:
        return None

    db_schema = DatabaseSchema.model_validate_json(db_schema_string, strict=True)
    for table in db_schema.tables:
        table.name = sanitize_table_name(table.name)
        for column in table.columns:
            column.name = sanitize_column_name(column.name)

    return db_schema


def get_db_type(extension: str) -> str:
    if extension == ".csv" or extension == ".tsv":
        return "csv"
    elif extension == ".xlsx" or extension == ".xlsm" or extension == ".xlsb":
        return "excel"
    return ""


def sanitize_table_configs(configs: Dict) -> Dict[str, TableConfig]:
    sanitized_configs = {}
    for key, config in configs.items():
        excel_params = parse_excel_range(config["excel_range"])
        sanitized_configs[key] = TableConfig(
            name=sanitize_table_name(config["new_name"]),
            excel_params=excel_params,
        )
    return sanitized_configs


def parse_excel_range(excel_range: str) -> Dict:
    """
    Expects Excel row and column range in the format of "colrow:colrow" e.g. "A1:F9".
    Returns pandas read_excel's parameters including header, nrows and usecols.
    """
    try:
        start, end = excel_range.split(":")[:2]
        start_row, start_col = parse_row_col(start)
        end_row, end_col = parse_row_col(end)
        return {
            "header": start_row - 1,
            "nrows": end_row - start_row,
            "usecols": f"{start_col}:{end_col}",
        }
    except:
        return {}


def parse_row_col(literal: str) -> Tuple[str, str]:
    index = 0
    for i, char in enumerate(literal):
        if char.isdigit():
            index = i
            break
    return int(literal[index:]), literal[:index]


def merge_db_files(db_files: List):
    dfs = []
    for db_file in db_files:
        delimiter = "," if Path(db_file.file.name).suffix == ".csv" else "\t"
        df = pd.read_csv(db_file.file, sep=delimiter)
        df.insert(0, "Trial", Path(db_file.file.name).stem)
        dfs.append(df)

    csv_buffer = io.StringIO()
    merged_df = pd.concat(dfs, ignore_index=True)
    merged_df.to_csv(csv_buffer, index=False)
    return csv_buffer
