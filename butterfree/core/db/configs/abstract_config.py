"""Abstract classes for database configurations with spark."""

from abc import ABC, abstractmethod, abstractproperty


class AbstractWriteConfig(ABC):
    """Abstract class for database write configurations with spark."""

    @abstractproperty
    def mode(self) -> str:
        """Config option "mode" for spark write.

        Returns:
            str: mode.
        """

    @abstractproperty
    def format_(self) -> str:
        """Config option "format" for spark write.

        Returns:
            str: format.
        """

    @abstractmethod
    def get_options(self, *args, **kwargs) -> dict:
        """Additional options for spark write.

        Returns:
            dict: key/value structure with additional options.
        """
