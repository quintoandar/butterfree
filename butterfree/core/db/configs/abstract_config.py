"""Abstract class for database write configurations with spark."""

from abc import ABC, abstractproperty


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

    @abstractproperty
    def options(self) -> dict:
        """Additional options for spark write.

        Returns:
            dict: key/value structure with additional options.
        """
