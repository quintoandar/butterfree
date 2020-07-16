"""Abstract class for database clients."""
from abc import ABC, abstractmethod


class AbstractClient(ABC):
    """Abstract base class for database clients."""

    @property
    @abstractmethod
    def conn(self):
        """Returns a connection object."""
        pass

    @abstractmethod
    def sql(self, query: str):
        """Runs a query.

        Args:
          query: client query.

        Returns:
            Set of records.
        """
        pass
