"""Holds the Online Feature Store writer class."""

import os
from typing import Any, List, Optional

from pyspark.sql import DataFrame, Window
from pyspark.sql.functions import col, row_number
from pyspark.sql.streaming import StreamingQuery

from butterfree.core.clients import SparkClient
from butterfree.core.configs.db import CassandraConfig
from butterfree.core.constants.columns import TIMESTAMP_COLUMN
from butterfree.core.load.writers.writer import Writer
from butterfree.core.transform import FeatureSet


class OnlineFeatureStoreWriter(Writer):
    """Enable writing feature sets into the Online Feature Store.

    Attributes:
        db_config: Spark configuration for connect databases.
            For more information check the module 'butterfree.core.db.configs'.
        debug_mode: "dry run" mode, write the result to a temporary view.

    Example:
        Simple example regarding OnlineFeatureStoreWriter class instantiation.
        We can instantiate this class without db configurations, so the class get the
        CassandraConfig() where it provides default configurations about CassandraDB.
    >>> spark_client = SparkClient()
    >>> writer = OnlineFeatureStoreWriter()
    >>> writer.write(feature_set=feature_set,
       ...           dataframe=dataframe,
       ...           spark_client=spark_client)

        However, we can define the db configurations and provide them to
        OnlineFeatureStoreWriter.
    >>> spark_client = SparkClient()
    >>> config = CassandraConfig(mode="overwrite",
        ...                      format_="parquet",
        ...                      keyspace="keyspace_name")

    >>> writer = OnlineFeatureStoreWriter(db_config=config)
    >>> writer.write(feature_set=feature_set,
       ...           dataframe=dataframe,
       ...           spark_client=spark_client)
        For what settings you can use on CassandraConfig and default settings,
        to read CassandraConfig class.

        We can instantiate OnlineFeatureStoreWriter class to validate the writers,
        using the default or custom configs.
    >>> spark_client = SparkClient()
    >>> writer = OnlineFeatureStoreWriter()
    >>> writer.validate(feature_set=feature_set,
       ...              dataframe=dataframe,
       ...              spark_client=spark_client)

        Both methods (writer and validate) will need the Spark Client,
        Feature Set and DataFrame, to write or to validate,
        according to OnlineFeatureStoreWriter class arguments.
    """

    def __init__(self, db_config=None, debug_mode: bool = False):
        self.db_config = db_config or CassandraConfig()
        self.debug_mode = debug_mode

    @staticmethod
    def filter_latest(dataframe: DataFrame, id_columns: List[Any]):
        """Filters latest data from the dataframe.

        Args:
            dataframe: spark dataframe containing data from a feature set.
            id_columns: unique identifier column set for this feature set.

        Returns:
            dataframe: contains only latest data for each unique id in the
                feature set.

        """
        if TIMESTAMP_COLUMN not in dataframe.columns:
            raise KeyError("DataFrame must have a 'ts' column to order by.")
        if id_columns is None or not id_columns:
            raise ValueError("Users must provide the unique identifiers.")
        missing = [c for c in id_columns if c not in dataframe.columns]
        if missing:
            raise KeyError(f"{missing} not found in the DataFrame.")

        window = Window.partitionBy(*id_columns).orderBy(col(TIMESTAMP_COLUMN).desc())
        return (
            dataframe.select(col("*"), row_number().over(window).alias("rn"),)
            .filter(col("rn") == 1)
            .drop("rn")
        )

    def write(
        self, feature_set: FeatureSet, dataframe: DataFrame, spark_client: SparkClient,
    ) -> Optional[StreamingQuery]:
        """Loads the latest data from a feature set into the Feature Store.

        Args:
            feature_set: object processed with feature set metadata.
            dataframe: Spark dataframe containing data from a feature set.
            spark_client: client for Spark connections with external services.

        Returns:
            Streaming handler if writing streaming df, None otherwise.

        If the debug_mode is set to True, a temporary table with a name in the format:
        online_feature_store__{feature_set.name} will be created instead of writing to
        the real online feature store.

        """
        if dataframe.isStreaming:
            if self.debug_mode:
                raise NotImplementedError(
                    "Writing stream df in debug_mod is not implemented yet."
                )

            checkpoint_path = (
                os.path.join(
                    self.db_config.stream_checkpoint_path,
                    feature_set.entity,
                    feature_set.name,
                )
                if self.db_config.stream_checkpoint_path
                else None
            )
            for table in [feature_set.name, feature_set.entity]:
                streaming_handler = spark_client.write_stream(
                    dataframe,
                    processing_time=self.db_config.stream_processing_time,
                    output_mode=self.db_config.stream_output_mode,
                    checkpoint_path=checkpoint_path,
                    format_=self.db_config.format_,
                    mode=self.db_config.mode,
                    **self.db_config.get_options(table=table),
                )
                return streaming_handler

        dataframe = self.filter_latest(
            dataframe=dataframe, id_columns=feature_set.keys_columns
        )

        if self.debug_mode:
            spark_client.create_temporary_view(
                name=f"online_feature_store__{feature_set.name}", dataframe=dataframe
            )
            return

        for table in [feature_set.name, feature_set.entity]:
            spark_client.write_dataframe(
                dataframe=dataframe,
                format_=self.db_config.format_,
                mode=self.db_config.mode,
                **self.db_config.get_options(table=table),
            )

    def validate(self, feature_set: FeatureSet, dataframe, spark_client: SparkClient):
        """Calculate dataframe rows to validate data into Feature Store.

        Args:
            feature_set: object processed with feature set metadata.
            dataframe: Spark dataframe containing data from a feature set.
            spark_client: client for Spark connections with external services.

        Raises:
            AssertionError: if validation fails.

        """
        # validation with online feature store can be done right now, since
        # this database can be updated by an streaming job while our ETL is running
        # therefore, creating in consistencies between this ETL feature set count and
        # the data already written to the FS.
        # TODO how to run data validations when a database has concurrent writes.
        pass

    def get_db_schema(self, feature_set: FeatureSet):
        """Get desired database schema.

        Args:
            feature_set: object processed with feature set metadata.

        Returns:
            Desired database schema.

        """
        db_schema = self.db_config.translate(feature_set.get_schema())
        return db_schema
