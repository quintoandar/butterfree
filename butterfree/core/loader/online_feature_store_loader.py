"""Holds the Online Feature Store loader class."""

from typing import Any, List

from pyspark.sql import DataFrame, Window
from pyspark.sql.functions import col, row_number

from butterfree.core.client import SparkClient
from butterfree.core.constant.columns import TIMESTAMP_COLUMN
from butterfree.core.db.configs import CassandraWriteConfig


class OnlineFeatureStoreLoader:
    """Enable writing feature sets into the Online Feature Store.

    Attributes:
        spark_client: client for spark connections with external services.
        db_config:
    """

    def __init__(self, spark_client: SparkClient, db_config=None):
        self.spark_client = spark_client
        self.db_config = db_config or CassandraWriteConfig()

    @staticmethod
    def filter_latest(dataframe: DataFrame, id_columns: List[Any]):
        """Filters latest data from the dataframe.

        Args:
            dataframe: spark dataframe containing data from a feature set.
            id_columns: unique identifier column set for this feature set.

        Returns:
            dataframe: contains only latest data for each unique id in the feature set.
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

    def load(self, dataframe: DataFrame, name: str, id_columns: List[Any]):
        """Loads the latest data from a feature set into the Online Feature Store.

        Args:
            dataframe: spark dataframe containing data from a feature set.
            name: feature set name.
            id_columns: unique identifier column set for this feature set.
        """
        dataframe = self.filter_latest(dataframe=dataframe, id_columns=id_columns)
        self.spark_client.write_dataframe(
            dataframe,
            format=self.db_config.format_,
            options=self.db_config.get_options(table=name),
        )
