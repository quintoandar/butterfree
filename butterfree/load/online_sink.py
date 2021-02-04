"""Holds the Sink class."""
from typing import Any, List

from pyspark.sql import DataFrame, Window
from pyspark.sql.functions import col, row_number
from pyspark.sql.streaming import StreamingQuery

from butterfree.clients import SparkClient
from butterfree.constants.columns import TIMESTAMP_COLUMN
from butterfree.load.writers.writer import Writer
from butterfree.transform import FeatureSet


class OnlineSink:
    """Define the destinations for the feature set pipeline.

    A Sink is created from a set of writers. The main goal of the Sink is to
    trigger the load in each defined writers. After the load the entity can be
    used to make sure that all data was written properly using the validate
    method.

    Attributes:
        writers: list of Writers to use to load the data.

    """

    def __init__(self, writers: List[Writer], filter_latest: bool = True):
        self.writers = writers
        self.filter_latest = filter_latest

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

    @staticmethod
    def filter(dataframe: DataFrame, id_columns: List[Any]) -> DataFrame:
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

    def flush(
        self, feature_set: FeatureSet, dataframe: DataFrame, spark_client: SparkClient
    ) -> List[StreamingQuery]:
        """Trigger a write job in all the defined Writers.

        Args:
            dataframe: spark dataframe containing data from a feature set.
            feature_set: object processed with feature set metadata.
            spark_client: client used to run a query.

        Returns:
            Streaming handlers for each defined writer, if writing streaming dfs.

        """
        if self.filter_latest:
            dataframe = self.filter(
                dataframe=dataframe, id_columns=feature_set.keys_columns
            )

        handlers = [
            writer.write(
                feature_set=feature_set, dataframe=dataframe, spark_client=spark_client
            )
            for writer in self.writers
        ]

        return [handler for handler in handlers if handler]
