import logging
from pathlib import Path

from google.cloud import bigquery
from google.cloud.exceptions import NotFound
from google.oauth2 import service_account
from pandas import DataFrame

from deepdive.database.client import DatabaseClient
from deepdive.models import Database

SERVICE_ACCOUNT_FILE = "bigquery_service_account.json"

logger = logging.getLogger(__name__)


def _get_bigquery_client():
    credentials = service_account.Credentials.from_service_account_file(
        Path(SERVICE_ACCOUNT_FILE).resolve(),
        scopes=["https://www.googleapis.com/auth/cloud-platform"],
    )
    return bigquery.Client(credentials=credentials, project=credentials.project_id)


class BigQueryClient(DatabaseClient):
    def initialize(self, database: Database):
        self.database = database
        self.client = _get_bigquery_client()
        self.job_config = bigquery.QueryJobConfig(
            default_dataset=database.bigquery_dataset_id
        )

    def finalize(self):
        pass

    def validate(database: Database):
        if not database.bigquery_dataset_id:
            raise Exception("BigQuery dataset ID not set!")
        try:
            _get_bigquery_client().get_dataset(database.bigquery_dataset_id)
        except NotFound:
            raise Exception(
                f"BigQuery dataset ID invalid: {database.bigquery_dataset_id}"
            )

    def execute_query(self, query: str) -> DataFrame:
        return self.client.query(query, job_config=self.job_config).to_dataframe()
