"""Abstract Validation class."""
from abc import ABC, abstractmethod

from pyspark.sql.dataframe import DataFrame


class Validation(ABC):
    """Validate dataframe properties.

    Attributes:
        dataframe: data to be verified.

    """

    def __init__(self, dataframe: DataFrame = None):
        self.dataframe = dataframe

    def input(self, dataframe: DataFrame) -> "Validation":
        """Input a dataframe to check.

        Args:
            dataframe: data to check.

        """
        self.dataframe = dataframe
        return self

    @abstractmethod
    def check(self) -> None:
        """Check validation properties about the dataframe.

        Raises:
            ValueError: if any of the verifications fail.

        """
