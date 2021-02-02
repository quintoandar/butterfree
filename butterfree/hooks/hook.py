"""Hook abstract class entity."""

from abc import ABC, abstractmethod

from pyspark.sql import DataFrame


class Hook(ABC):
    """Definition of a hook function to call on a Dataframe."""

    @abstractmethod
    def run(self, dataframe: DataFrame) -> DataFrame:
        """Run interface for Hook.

        Args:
            dataframe: dataframe to use in the Hook.

        Returns:
            dataframe result from the Hook.
        """
