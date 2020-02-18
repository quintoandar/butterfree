"""Holds the Historical Feature Store writer class."""

import os

from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import dayofmonth, month, year

from butterfree.core.clients import SparkClient
from butterfree.core.configs import environment
from butterfree.core.configs.db import S3Config
from butterfree.core.constants import columns
from butterfree.core.load.writers.writer import Writer
from butterfree.core.transform import FeatureSet
from butterfree.core.validations.validate_dataframe import ValidateDataframe


class HistoricalFeatureStoreWriter(Writer):
    """Enable writing feature sets into the Historical Feature Store.

    Attributes:
        db_config: configuration with spark for databases or AWS S3 (default).
            For more information check module 'butterfree.core.db.configs'.

    """

    PARTITION_BY = [
        columns.PARTITION_YEAR,
        columns.PARTITION_MONTH,
        columns.PARTITION_DAY,
    ]

    def __init__(self, db_config=None, database=None):
        self.db_config = db_config or S3Config()
        self.database = database or environment.get_variable(
            "FEATURE_STORE_HISTORICAL_DATABASE"
        )

    def write(
        self, feature_set: FeatureSet, dataframe: DataFrame, spark_client: SparkClient
    ):
        """Loads the data from a feature set into the Historical Feature Store.

        Args:
            feature_set: object processed with feature_set informations.
            dataframe: spark dataframe containing data from a feature set.
            spark_client: client for spark connections with external services.

        """
        s3_key = os.path.join("historical", feature_set.entity, feature_set.name)

        validate_dataframe = ValidateDataframe(dataframe)
        validate_dataframe.checks()
        dataframe = self._create_partitions(dataframe).repartition(*self.PARTITION_BY)

        spark_client.write_table(
            dataframe=dataframe,
            database=self.database,
            table_name=feature_set.name,
            partition_by=self.PARTITION_BY,
            **self.db_config.get_options(s3_key),
        )

    def validate(
        self, feature_set: FeatureSet, dataframe: DataFrame, spark_client: SparkClient
    ):
        """Calculate dataframe rows to validate data into Feature Store.

        Args:
            feature_set: object processed with feature_set informations.
            dataframe: spark dataframe containing data from a feature set.
            spark_client: client for spark connections with external services.

        Returns:
            Boolean indicating count of written data matches count in current feature
                set dataframe.

        """
        table_name = "{}.{}".format(self.database, feature_set.name)
        query_format_string = "SELECT COUNT(1) as row FROM {}"
        query_count = query_format_string.format(table_name)

        written_count = spark_client.sql(query=query_count).collect().pop()["row"]
        dataframe_count = dataframe.count()

        return written_count == dataframe_count

    @staticmethod
    def _create_partitions(dataframe):
        # create year partition column
        dataframe = dataframe.withColumn(
            columns.PARTITION_YEAR, year(dataframe[columns.TIMESTAMP_COLUMN])
        )
        # create month partition column
        dataframe = dataframe.withColumn(
            columns.PARTITION_MONTH, month(dataframe[columns.TIMESTAMP_COLUMN])
        )
        # create day partition column
        dataframe = dataframe.withColumn(
            columns.PARTITION_DAY, dayofmonth(dataframe[columns.TIMESTAMP_COLUMN])
        )
        return dataframe
