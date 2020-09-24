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
        debug_mode: "dry run" mode, write the result to a temporary view.
        write_to_entity: option to write the data to the entity table.
            With this option set to True, the writer will write the feature set to
            a table with the name equal to the entity name, defined on the pipeline.
            So, it WILL NOT write to a table with the name of the feature set, as it
            normally does.

    """

    def __init__(self, debug_mode: bool = False):
        super().__init__()
        self.enable_post_hooks = False
        self.debug_mode = debug_mode

    @abstractmethod
    def load(
        self, feature_set: FeatureSet, dataframe: DataFrame, spark_client: SparkClient,
    ):
        """Loads the latest data from a feature set into the Feature Store.

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

    @abstractmethod
    def check_schema(self, client, table_name, database=None):
        """Instantiate the schema check hook and add it to the list of pre hooks.

        Args:
            client: client for Spark or Cassandra connections with external services.
            table_name: table name where the dataframe will be saved.
            database: database name where the dataframe will be saved.
        """

    def write(
        self,
        spark_client: SparkClient,
        load_df: DataFrame,
        table_name: str,
        db_config,
        options: dict,
        partition_by=None,
    ):
        """Save data from a feature set into the Feature Store.

        Args:
            spark_client: client for Spark connections with external services.
            load_df: Dataframe ready to be saved.
            table_name: Table name where the dataframe will be saved.
            db_config: Spark configuration for connect databases.
            options(optional = None): All other string options.
            partition_by(optional = None): Partition column to use when writing.

        If the debug_mode is set to True, a temporary table with a name in the format:
        {feature_set.name} will be created instead of writing
        to the real historical or online feature store.
        """
        if self.debug_mode:
            return self.write_in_debug_mode(
                table_name=(
                    f"historical_feature_store__{table_name}"
                    if partition_by
                    else f"online_feature_store__{table_name}"
                ),
                dataframe=load_df,
                spark_client=spark_client,
            )

        spark_client.write_dataframe(
            dataframe=load_df,
            format_=db_config.format_,
            mode=db_config.mode,
            **options,
            partitionBy=partition_by,
        )

    @staticmethod
    def write_in_debug_mode(
        table_name: str, dataframe: DataFrame, spark_client: SparkClient
    ) -> Optional[StreamingQuery]:
        """Creates a temporary table instead of writing to the real feature store."""
        return spark_client.create_temporary_view(
            dataframe=dataframe, name=f"{table_name}"
        )
