"""Writer entity."""
import os
from abc import ABC, abstractmethod
from typing import Optional

from pyspark.sql.dataframe import DataFrame
from pyspark.sql.streaming import StreamingQuery

from butterfree.clients import SparkClient
from butterfree.dataframe_service import extract_partition_values
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

    def __init__(self, debug_mode: bool = False, write_to_entity: bool = False):
        super().__init__()
        self.enable_post_hooks = False
        self.debug_mode = debug_mode
        self.write_to_entity = write_to_entity

    @abstractmethod
    def load(
        self, feature_set: FeatureSet, dataframe: DataFrame, spark_client: SparkClient,
    ):
        """Prepare the dataframe before it is saved to the Feature Store.

        Feature Store could be Online or Historical.

        Args:
            feature_set: object processed with feature set metadata.
            dataframe: Spark dataframe containing data from a feature set.
            spark_client: client for Spark connections with external services.

        Returns:
            load_df: Dataframe ready to be saved.
            db_config: Spark configuration for connect databases.
            options(optional = None): All other string options.
            database(optional = None): Database name where the dataframe will be saved.
            table_name: Table name where the dataframe will be saved.
            partition_by(optional = None): Partition column to use when writing.

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

    def write(
        self, feature_set: FeatureSet, dataframe: DataFrame, spark_client: SparkClient,
    ) -> Optional[StreamingQuery]:
        """Loads the latest data from a feature set into the Feature Store.

        Feature Store could be Online or Historical.

        Args:
            feature_set: object processed with feature set metadata.
            dataframe: Spark dataframe containing data from a feature set.
            spark_client: client for Spark connections with external services.

        If the debug_mode is set to True, a temporary table with a name in the format:
        {feature_set.name} will be created instead of writing
        to the real historical or online feature store.
        If dataframe is streaming this temporary table will be updated in real time.

        Returns:
            Streaming handler if writing streaming df, None otherwise.

        """
        load_df, db_config, options, database, table_name, partition_by = self.load(
            feature_set=feature_set, dataframe=dataframe, spark_client=spark_client,
        )

        pre_hook_df = self.run_pre_hooks(load_df)

        if self.debug_mode:
            return self._write_in_debug_mode(
                table_name=(
                    f"historical_feature_store__{table_name}"
                    if partition_by
                    else f"online_feature_store__{table_name}"
                ),
                dataframe=pre_hook_df,
                spark_client=spark_client,
            )

        if dataframe.isStreaming:
            return self._write_stream(
                feature_set=feature_set,
                dataframe=pre_hook_df,
                spark_client=spark_client,
                table_name=table_name,
                db_config=db_config,
            )

        spark_client.write_dataframe(
            dataframe=pre_hook_df,
            format_=db_config.format_,
            mode=db_config.mode,
            **options,
            partitionBy=partition_by,
        )

        if partition_by:
            partition_values = extract_partition_values(pre_hook_df, partition_by)

            spark_client.add_table_partitions(
                partition_values, feature_set.name, database
            )

    @staticmethod
    def _write_in_debug_mode(
        table_name: str, dataframe: DataFrame, spark_client: SparkClient
    ) -> Optional[StreamingQuery]:
        """Creates a temporary table instead of writing to the real feature store."""
        return spark_client.create_temporary_view(
            dataframe=dataframe, name=f"{table_name}"
        )

    def _write_stream(
        self,
        feature_set: FeatureSet,
        dataframe: DataFrame,
        spark_client: SparkClient,
        table_name: str,
        db_config,
    ) -> StreamingQuery:
        """Writes the dataframe in streaming mode."""
        checkpoint_folder = (
            f"{feature_set.name}__on_entity" if self.write_to_entity else table_name
        )
        checkpoint_path = (
            os.path.join(
                db_config.stream_checkpoint_path, feature_set.entity, checkpoint_folder,
            )
            if db_config.stream_checkpoint_path
            else None
        )
        streaming_handler = spark_client.write_stream(
            dataframe,
            processing_time=db_config.stream_processing_time,
            output_mode=db_config.stream_output_mode,
            checkpoint_path=checkpoint_path,
            format_=db_config.format_,
            mode=db_config.mode,
            **db_config.get_options(table=table_name),
        )
        return streaming_handler
