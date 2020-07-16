"""Abstract classes for database configurations with spark."""

from abc import ABC, abstractmethod, abstractproperty
from typing import Dict, List


class AbstractWriteConfig(ABC):
    """Abstract class for database write configurations with spark."""

    @abstractproperty
    def mode(self) -> str:
        """Config option "mode" for spark write.

        Args:

        Returns:
          str: mode.

        """

    @abstractproperty
    def format_(self) -> str:
        """Config option "format" for spark write.

        Args:

        Returns:
          str: format.

        """

    @abstractmethod
    def get_options(self, *args, **kwargs) -> dict:
        """Get connection options configuration defined in the entity.

        Args:
          *args: args to use in the options
          **kwargs: kwargs to use in the options

        Returns:
            Connection options configuration.

        """

    @abstractmethod
    def translate(self, schema) -> List[Dict]:
        """Translate feature set spark schema to the corresponding database.

        Args:
          schema: feature set schema

        Returns:
            Corresponding database schema.

        """
