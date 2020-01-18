"""Transform Abstract Class."""

from abc import ABC, abstractmethod

from pyspark.sql import DataFrame


class FeatureComponent(ABC):
    """Defines an abstract FeatureComponent."""

    def add(self, component) -> None:
        """Add a component to the desired pipeline."""
        raise NotImplementedError()

    @abstractmethod
    def transform(self, dataframe: DataFrame):
        """Base transform method.

        Args:
            dataframe: base dataframe.

        Returns:
            dataframe: transformed dataframe.
        """
        pass
