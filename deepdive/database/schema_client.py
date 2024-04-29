from abc import ABC, abstractmethod

from deepdive.models import Database
from deepdive.schema import DatabaseSchema


class SchemaClient(ABC):
    """
    For certain database types, we need to fetch the DB schema upon initialization
    and periodically (as of now: whenver a session for the DB is invoked)
    """

    def __init__(self, database: Database):
        self.initialize(database)

    @abstractmethod
    def initialize(self, database: Database):
        pass

    @abstractmethod
    def fetch(self) -> DatabaseSchema:
        pass
