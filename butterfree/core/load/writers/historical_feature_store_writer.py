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

    Example:
        Simple example regarding HistoricalFeatureStoreWriter class instantiation.
        We can instantiate this class without db configurations, so the class get the
        S3Config() where it provides default configurations about AWS S3 service.
    >>> spark_client = SparkClient()
    >>> writer = HistoricalFeatureStoreWriter()
    >>> writer.write(feature_set=feature_set,
       ...           dataframe=dataframe,
       ...           spark_client=spark_client)

        However, we can define the db configurations,
        like write mode, file format and S3 bucket,
        and provide them to HistoricalFeatureStoreWriter.
    >>> spark_client = SparkClient()
    >>> config = S3Config(bucket="wonka.s3.forno.data.quintoandar.com.br",
        ...               mode="append",
        ...               format_="parquet")

    >>> writer = HistoricalFeatureStoreWriter(db_config=config)
    >>> writer.write(feature_set=feature_set,
       ...           dataframe=dataframe,
       ...           spark_client=spark_client)
        For what settings you can use on S3Config and default settings,
        to read S3Config class.

        We can instantiate HistoricalFeatureStoreWriter class to validate the writers,
        using the default or custom configs.
    >>> spark_client = SparkClient()
    >>> writer = HistoricalFeatureStoreWriter()
    >>> writer.validate(feature_set=feature_set,
       ...              dataframe=dataframe,
       ...              spark_client=spark_client)

        Both methods (writer and validate) will need the Spark Client,
        Feature Set and DataFrame, to write or to validate, according to
        HistoricalFeatureStoreWriter class arguments.

        P.S.: When load, the HistoricalFeatureStoreWrite partitions
        the data to improving queries performance.
        The partition are stored in separate folders in AWS S3,
        and to partition the data based on time (per year, month and day).
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
        dataframe = self._create_partitions(dataframe)

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

        Raises:
            AssertionError: if count of written data doesn't match count in current
                feature set dataframe.

        """
        table_name = "{}.{}".format(self.database, feature_set.name)
        query_format_string = "SELECT COUNT(1) as row FROM {}"
        query_count = query_format_string.format(table_name)

        written_count = spark_client.sql(query=query_count).collect().pop()["row"]
        dataframe_count = dataframe.count()

        assert written_count == dataframe_count, (
            "Data written to the Historical Feature Store and read back "
            f"from {table_name} has a different count than the feature set dataframe. "
            f"\nNumber of rows in {table_name}: {written_count}."
            f"\nNumber of rows in the dataframe: {dataframe_count}."
        )

    def _create_partitions(self, dataframe):
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

        return dataframe.repartition(*self.PARTITION_BY)
