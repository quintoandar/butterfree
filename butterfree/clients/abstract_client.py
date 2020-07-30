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
