"""Holds the Historical Feature Store loader class."""

import os

from butterfree.core.client.spark_client import SparkClient
from butterfree.core.configs import environment
from butterfree.core.loader.verify_dataframe import verify_column_ts


class HistoricalFeatureStoreLoader:
    """Enable writing feature sets into the Historical Feature Store.

    Attributes:
        spark_client: client for spark connections with external services.
    """

    HISTORICAL_FEATURE_STORE_S3_PATH = (
        f"s3a://{environment.get_variable('FEATURE_STORE_S3_BUCKET')}/historical/"
    )
    DEFAULT_FORMAT = "parquet"
    DEFAULT_MODE = "overwrite"
    DEFAULT_PARTITION_BY = ["partition__year", "partition__month", "partition__day"]

    def __init__(self, spark_client: SparkClient):
        self.spark_client = spark_client

    def load(self, dataframe, name):
        """Loads the data from a feature set into the Historical Feature Store.

        Args:
            dataframe: spark dataframe containing data from a feature set.
            name: feature set name.
        """
        s3_path = os.path.join(self.HISTORICAL_FEATURE_STORE_S3_PATH, name)
        dataframe = verify_column_ts(dataframe)

        self.spark_client.write_table(
            dataframe=dataframe,
            name=name,
            format_=self.DEFAULT_FORMAT,
            mode=self.DEFAULT_MODE,
            partition_by=self.DEFAULT_PARTITION_BY,
            path=s3_path,
        )
