from pathlib import Path
from typing import Dict

import pandas as pd

from deepdive.database.file_based_client import FileBasedClient
from deepdive.models import DatabaseFile


class ParquetClient(FileBasedClient):
    """
    A DatabaseClient to handle parquet files.
    """

    def read_data(self, db_file: DatabaseFile) -> Dict[str, pd.DataFrame]:
        with db_file.file.open("r") as file:
            return {
                Path(file.name).stem: pd.read_parquet(file)
            }
