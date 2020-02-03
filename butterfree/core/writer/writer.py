"""Writer entity."""

from abc import ABC, abstractmethod

from pyspark.sql.dataframe import DataFrame

from butterfree.core.client import SparkClient
from butterfree.core.transform import FeatureSet


class Writer(ABC):
    """Abstract base class for Writers.

    Attributes:
        spark_client: client for spark connections with external services.
        db_config: object with access configuration to storage. More information
            about the module in 'butterfree.core.db.configs'.
    """

    def __init__(self, spark_client: SparkClient):
        self.spark_client = spark_client

    @abstractmethod
    def write(self, feature_set: FeatureSet, dataframe: DataFrame):
        """Loads the data from a feature set into the Feature Store.

        Feature Store could be Online or Historical.

        Args:
            feature_set: object processed with feature_set informations.
            dataframe: dataframe containing records from a feature set.
        """

    @abstractmethod
    def validate(self, feature_set: FeatureSet, dataframe: DataFrame):
        """Calculate metrics to validate data into Feature Store.

        Args:
            feature_set: object containing feature set metadata.
            dataframe: spark dataframe containing data from a feature set.
            feature_set: FeatureSet:

        Returns:
            True for success validation, False otherwise
        """
