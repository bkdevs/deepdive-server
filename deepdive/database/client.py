from abc import ABC, abstractmethod

from pandas import DataFrame

from deepdive.models import Database
from deepdive.schema import DatabaseSchema


class DatabaseClient(ABC):
    def __init__(self, database: Database):
        self.initialize(database)

    @staticmethod
    @abstractmethod
    def validate(database: Database):
        pass

    @abstractmethod
    def initialize(self, database: Database):
        pass

    @abstractmethod
    def finalize(self):
        pass

    @abstractmethod
    def execute_query(self, query: str) -> DataFrame:
        pass
