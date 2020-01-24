"""Writer entity."""

from abc import ABC, abstractmethod

from butterfree.core.client import SparkClient
from butterfree.core.transform import FeatureSet


class Writer(ABC):
    """Abstract base class for Writers."""

    def __init__(self, spark_client: SparkClient):
        self.spark_client = spark_client

    @abstractmethod
    def write(self, feature_set: FeatureSet):
        """Loads the data from a feature set into the Historical Feature Store."""

    @abstractmethod
    def validate(self, feature_set: FeatureSet):
        """Validate to load the feature set into Writer.

        Returns:
            False: fail validation.
            True: success validation.
        """
