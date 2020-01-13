"""Holds the Online Feature Store loader class."""

import os
from typing import Any, List

from pyspark.sql import DataFrame, Window
from pyspark.sql.functions import col, row_number

from butterfree.core.constant.columns import TIMESTAMP_COLUMN


class OnlineFeatureStoreLoader:
    """Enable writing feature sets into the Online Feature Store.

    Attributes:
        spark_client: client for spark connections with external services.
        format_: specific format to write in the online feature store.
    """

    CASSANDRA_KEYSPACE = os.getenv("CASSANDRA_KEYSPACE")
    CASSANDRA_USERNAME = os.getenv("CASSANDRA_USERNAME")
    CASSANDRA_PASSWORD = os.getenv("CASSANDRA_PASSWORD")
    CASSANDRA_HOST = os.getenv("CASSANDRA_HOST")
    CASSANDRA_FORMAT = "org.apache.spark.sql.cassandra"

    def __init__(self, spark_client, format_=None):
        self.spark_client = spark_client
        self.format_ = format_ or self.CASSANDRA_FORMAT

    def get_options(
        self, table, keyspace=None, username=None, password=None, host=None
    ):
        """Builds an option dictionary to be used by the spark client when writing data.

        Args:
            table: destination cassandra table to write data.
            keyspace: destination cassandra keyspace to write data.
            username: username to get access to the cassandra cluster.
            password: password to get access to the cassandra cluster.
            host: host to access the cassandra cluster.

        Returns:
            dict: contains proper options to configure spark when writing to cassandra.
        """
        if table is None:
            raise ValueError("User must set table destination before using options.")
        return {
            "table": table,
            "keyspace": keyspace or self.CASSANDRA_KEYSPACE,
            "spark.cassandra.auth.username": username or self.CASSANDRA_USERNAME,
            "spark.cassandra.auth.password": password or self.CASSANDRA_PASSWORD,
            "spark.cassandra.connection.host": host or self.CASSANDRA_HOST,
        }

    @staticmethod
    def filter_latest(dataframe: DataFrame, id_columns: List[Any]):
        """Filters latest data from the dataframe.

        Args:
            dataframe: spark dataframe containing data from a feature set.
            id_columns: unique identifier column set for this feature set.

        Returns:
            dataframe: contains only latest data for each unique id in the feature set.
        """
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
            dataframe, format=self.format_, options=self.get_options(table=name)
        )
