"""Abstract class for database clients."""
from abc import ABC, abstractmethod
from typing import Any


class AbstractClient(ABC):
    """Abstract base class for database clients."""

    @property
    @abstractmethod
    def conn(self) -> Any:
        """Returns a connection object."""
        pass

    @abstractmethod
    def sql(self, query: str) -> Any:
        """Runs a query.

        Args:
          query: client query.

        Returns:
            Set of records.
        """
        pass

    @abstractmethod
    def get_schema(self, table: str, database: str = None) -> Any:
        """Returns desired table schema.

        Attributes:
            table: desired table.

        Returns:
            A list of dictionaries in the format
            [{"column_name": "example1", type: "Spark_type"}, ...]

        """
        pass
