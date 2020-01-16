"""Transform Abstract Class."""

from abc import ABC, abstractmethod
from pyspark.sql import DataFrame


class FeatureComponent(ABC):
    @abstractmethod
    def transform(self, dataframe: DataFrame):
        """Apply transformations"""
        pass
