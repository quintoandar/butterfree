"""Holds the Sink class."""
from typing import List

from pyspark.sql import DataFrame
from pyspark.sql.functions import dayofmonth, month, year

from butterfree.clients import SparkClient
from butterfree.constants import columns
from butterfree.constants.spark_constants import DEFAULT_NUM_PARTITIONS
from butterfree.dataframe_service import repartition_df
from butterfree.load.writers.writer import Writer
from butterfree.transform import FeatureSet


class HistoricalSink:
    """Define the destinations for the feature set pipeline.

    A Sink is created from a set of writers. The main goal of the Sink is to
    trigger the load in each defined writers. After the load the entity can be
    used to make sure that all data was written properly using the validate
    method.

    Attributes:
        writers: list of Writers to use to load the data.

    """

    PARTITION_BY = [
        columns.PARTITION_YEAR,
        columns.PARTITION_MONTH,
        columns.PARTITION_DAY,
    ]

    def __init__(self, writers: List[Writer], num_partitions: int = None,):
        self.writers = writers
        self.num_partitions = num_partitions or DEFAULT_NUM_PARTITIONS

    @property
    def writers(self) -> List[Writer]:
        """List of Writers to use to load the data."""
        return self._writers

    @writers.setter
    def writers(self, value: List[Writer]) -> None:
        if not value or not all(isinstance(writer, Writer) for writer in value):
            raise ValueError("Writers needs to be a list of Writer instances.")
        else:
            self._writers = value

    def create_partitions(self, dataframe: DataFrame) -> DataFrame:
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
        return repartition_df(dataframe, self.PARTITION_BY, self.num_partitions)

    def flush(
        self, feature_set: FeatureSet, dataframe: DataFrame, spark_client: SparkClient
    ):
        """Trigger a write job in all the defined Writers.

        Args:
            dataframe: spark dataframe containing data from a feature set.
            feature_set: object processed with feature set metadata.
            spark_client: client used to run a query.

        Returns:
            Streaming handlers for each defined writer, if writing streaming dfs.

        """
        dataframe = self.create_partitions(
                dataframe=dataframe
            )

        handlers = [
            writer.write(
                feature_set=feature_set, dataframe=dataframe, spark_client=spark_client
            )
            for writer in self.writers
        ]

        return [handler for handler in handlers if handler]
