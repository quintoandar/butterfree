"""Writer entity."""

from __future__ import annotations

from abc import ABC, abstractmethod

from pyspark.sql.dataframe import DataFrame

from butterfree.clients import SparkClient
from butterfree.dataframe_service.incremental_strategy import IncrementalStrategy
from butterfree.transform import FeatureSet


class Writer(ABC):
    """Abstract base class for Writers.

    Args:
        spark_client: client for spark connections with external services.

    """

    def __init__(self, incremental_strategy: IncrementalStrategy = None):
        self.incremental_strategy = incremental_strategy

    @abstractmethod
    def write(
        self,
        feature_set: FeatureSet,
        dataframe: DataFrame,
        spark_client: SparkClient,
        start_date: str = None,
        end_date: str = None,
    ):
        """Loads the data from a feature set into the Feature Store.

        Feature Store could be Online or Historical.

        Args:
            feature_set: object processed with feature set metadata.
            dataframe: Spark dataframe containing data from a feature set.
            spark_client: client for Spark connections with external services.
            start_date: start date regarding the load layer.
            end_date: end date related to the load layer.

        """

    def with_incremental_strategy(
        self, incremental_strategy: IncrementalStrategy
    ) -> Writer:
        """Define the incremental strategy for the Writer.

        Args:
            incremental_strategy: definition of the incremental strategy.

        Returns:
            Writer with defined incremental strategy.

        """
        self.incremental_strategy = incremental_strategy
        return self

    @abstractmethod
    def validate(
        self, feature_set: FeatureSet, dataframe: DataFrame, spark_client: SparkClient
    ):
        """Calculate dataframe rows to validate data into Feature Store.

        Args:
            feature_set: object processed with feature set metadata.
            dataframe: Spark dataframe containing data from a feature set.
            spark_client: client for Spark connections with external services.

        Raises:
            AssertionError: if validation fails.

        """
