import json
from typing import Dict, List

import pandas as pd
from django.core.files.uploadedfile import UploadedFile

from deepdive.database.file_based_client import FileBasedClient
from deepdive.database.file_based_client_helper import (
    NUM_SAMPLE_ROWS,
    create_table_schema,
    sanitize_table_name,
)
from deepdive.models import DatabaseFile
from deepdive.schema import TableConfig, TablePreview


class ExcelClient(FileBasedClient):
    """
    A DatabaseClient to handle Excel files.
    """

    def preview_tables(uploaded_file: UploadedFile) -> List[TablePreview]:
        previews = []
        dfs = pd.read_excel(uploaded_file, sheet_name=None, nrows=NUM_SAMPLE_ROWS)
        for df in dfs.values():
            df.columns = map(str, df.columns)
        for table_name, sample_data in dfs.items():
            table_schema = create_table_schema(table_name, sample_data)
            sample_data.columns = [str(i) for i in range(0, len(table_schema.columns))]
            previews.append(
                TablePreview(
                    table_schema=table_schema,
                    sample_data=sample_data.to_json(orient="table", index=True),
                )
            )
        return previews

    def preview_table(
        uploaded_file: UploadedFile, sanitized_orig_table_name: str, config: TableConfig
    ) -> TablePreview:
        excel_file = pd.ExcelFile(uploaded_file)
        target_sheet_name = next(
            sheet_name
            for sheet_name in excel_file.sheet_names
            if sanitize_table_name(sheet_name) == sanitized_orig_table_name
        )
        df = excel_file.parse(sheet_name=target_sheet_name, **config.excel_params)
        df.columns = map(str, df.columns)

        table_schema = create_table_schema(config.name, df)
        sample_data = df.head(NUM_SAMPLE_ROWS)
        sample_data.columns = [str(i) for i in range(0, len(table_schema.columns))]
        return TablePreview(
            table_schema=table_schema,
            sample_data=sample_data.to_json(orient="table", index=True),
        )

    def read_data(self, db_file: DatabaseFile) -> Dict[str, pd.DataFrame]:
        configs = json.loads(db_file.configs)
        excel_file = pd.ExcelFile(db_file.file)
        table_schemas = {
            table_schema.name: table_schema for table_schema in self.db_schema.tables
        }
        data = {}
        for sheet_name in excel_file.sheet_names:
            sanitized_sheet_name = sanitize_table_name(sheet_name)
            config = TableConfig.model_validate_json(
                json.dumps(configs[sanitized_sheet_name])
            )
            table_name = config.name
            if table_name in table_schemas:
                df = excel_file.parse(sheet_name=sheet_name, **config.excel_params)
                df.columns = list(map(lambda col : col.name, table_schemas[table_name].columns))
                data[table_name] = df
        return data
