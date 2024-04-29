from pathlib import Path
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
from deepdive.schema import TablePreview


class CSVClient(FileBasedClient):
    """
    A DatabaseClient to handle CSV files.
    """

    def preview_tables(uploaded_file: UploadedFile) -> List[TablePreview]:
        delimiter = "," if Path(uploaded_file.name).suffix == ".csv" else "\t"
        sample_data = pd.read_csv(uploaded_file, nrows=NUM_SAMPLE_ROWS + 1, sep=delimiter)
        table_schema = create_table_schema(
            sanitize_table_name(Path(uploaded_file.name).stem), sample_data
        )
        sample_data.columns = [str(i) for i in range(0, len(table_schema.columns))]
        return [
            TablePreview(
                table_schema=table_schema,
                sample_data=sample_data.to_json(orient="table", index=True),
            )
        ]

    def read_data(self, db_file: DatabaseFile) -> Dict[str, pd.DataFrame]:
        table_name = self.db_schema.tables[0].name
        delimiter = "," if Path(db_file.file.name).suffix == ".csv" else "\t"
        return {
            table_name: pd.read_csv(
                db_file.file,
                names=self._get_column_names(table_name),
                header=None,
                skiprows=1,
                sep=delimiter,
            ),
        }

    def _get_column_names(self, table_name: str) -> List[str]:
        for table in self.db_schema.tables:
            if table.name == table_name:
                return [column.name for column in table.columns]

        raise Exception(f"Could not find table schema for table: {table_name}")
