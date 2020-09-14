"""Writer entity."""

from abc import ABC, abstractmethod

from pyspark.sql.dataframe import DataFrame

from butterfree.clients import SparkClient
from butterfree.hooks import HookableComponent
from butterfree.transform import FeatureSet


class Writer(ABC, HookableComponent):
    """Abstract base class for Writers.

    Args:
        spark_client: client for spark connections with external services.

    """

    def __init__(self, debug_mode: bool = False):
        super().__init__()
        self.debug_mode = debug_mode

    @abstractmethod
    def write(
        self, feature_set: FeatureSet, dataframe: DataFrame, spark_client: SparkClient,
    ):
        """Loads the data from a feature set into the Feature Store.

        Feature Store could be Online or Historical.

        Args:
            feature_set: object processed with feature set metadata.
            dataframe: Spark dataframe containing data from a feature set.
            spark_client: client for Spark connections with external services.

        """

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

    def build(
        self, feature_set: FeatureSet, dataframe: DataFrame, spark_client: SparkClient,
    ):
        """Register the data got from the reader in the Spark metastore.

        Create a temporary view in Spark metastore referencing the data
        extracted from the target origin after the application of all the
        defined pre-processing transformations.

        The arguments start_date and end_date are going to be use only when there
        is a defined `IncrementalStrategy` on the `Reader`.

        Args:
            client: client responsible for connecting to Spark session.
            columns: list of tuples for selecting/renaming columns on the df.
            start_date: lower bound to use in the filter expression.
            end_date: upper bound to use in the filter expression.

        """
        pre_hook_df = self.run_pre_hooks(dataframe)

        if self.debug_mode:
            spark_client.create_temporary_view(
                dataframe=dataframe,
                name=f"historical_feature_store__{feature_set.name}",
            )
            return

        self.write(
            feature_set=feature_set,
            dataframe=pre_hook_df,
            spark_client=spark_client,
        )
