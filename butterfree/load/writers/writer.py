"""Writer entity."""

from abc import ABC, abstractmethod
from typing import Optional

from pyspark.sql.dataframe import DataFrame
from pyspark.sql.streaming import StreamingQuery

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
        self.enable_post_hooks = False
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
    ) -> Optional[StreamingQuery]:
        """Register the data got from the reader in the Spark metastore.

        Create a temporary view in Spark metastore referencing the data
        extracted from the target origin after the application of all the
        defined pre-processing transformations.

        The arguments start_date and end_date are going to be use only when there
        is a defined `IncrementalStrategy` on the `Reader`.

        Args:
            feature_set: object processed with feature set metadata.
            dataframe: Spark dataframe containing data from a feature set.
            spark_client: client for Spark connections with external services.

        """
        pre_hook_df = self.run_pre_hooks(dataframe)

        if self.debug_mode:
            return self._write_in_debug_mode(
                table_name=f"{feature_set.name}",
                dataframe=pre_hook_df,
                spark_client=spark_client,
            )

        self.write(
            feature_set=feature_set, dataframe=pre_hook_df, spark_client=spark_client,
        )

    @staticmethod
    def _write_in_debug_mode(
        table_name: str, dataframe: DataFrame, spark_client: SparkClient
    ) -> Optional[StreamingQuery]:
        """Creates a temporary table instead of writing to the real feature store."""
        return spark_client.create_temporary_view(
            dataframe=dataframe, name=f"{table_name}"
        )
