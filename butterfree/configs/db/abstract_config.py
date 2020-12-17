"""Abstract classes for database configurations with spark."""

from abc import ABC, abstractmethod
from typing import Any, Dict, List


class AbstractWriteConfig(ABC):
    """Abstract class for database write configurations with spark."""

    @property
    @abstractmethod
    def mode(self) -> Any:
        """Config option "mode" for spark write.

        Args:

        Returns:
          str: mode.

        """

    @property
    @abstractmethod
    def format_(self) -> Any:
        """Config option "format" for spark write.

        Args:

        Returns:
          str: format.

        """

    @abstractmethod
    def translate(self, schema: Any) -> List[Dict[Any, Any]]:
        """Translate feature set spark schema to the corresponding database.

        Args:
          schema: feature set schema

        Returns:
            Corresponding database schema.

        """
