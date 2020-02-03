"""Writer entity."""

from abc import ABC, abstractmethod

from pyspark.sql.dataframe import DataFrame

from butterfree.core.client import SparkClient
from butterfree.core.transform import FeatureSet


class Writer(ABC):
    """Abstract base class for Writers.

    Args:
        spark_client: client for spark connections with external services.
    """

    @abstractmethod
    def write(
        self, feature_set: FeatureSet, dataframe: DataFrame, spark_client: SparkClient
    ):
        """Loads the data from a feature set into the Feature Store.

        Feature Store could be Online or Historical.

        Args:
            feature_set: object processed with feature_set informations.
            dataframe: spark dataframe containing data from a feature set.
            spark_client: client for spark connections with external services.
        """

    @abstractmethod
    def validate(
        self, feature_set: FeatureSet, dataframe: DataFrame, spark_client: SparkClient
    ):
        """Calculate dataframe rows to validate data into Feature Store.

        Args:
            feature_set: object processed with feature_set informations.
            dataframe: spark dataframe containing data from a feature set.
            spark_client: client for spark connections with external services.

        Returns:
            False: fail validation.
            True: success validation.
        """
