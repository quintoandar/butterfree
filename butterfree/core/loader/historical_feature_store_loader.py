import os
from pyspark.sql import DataFrame

from butterfree.core.loader.verify_dataframe import verify_column_ts
from butterfree.core.client.spark_client import SparkClient
from butterfree.core.configs import environment


class HistoricalFeatureStoreLoader:
    HISTORICAL_FEATURE_STORE_S3_PATH = f"s3a://{environment.get_variable('FEATURE_STORE_S3_BUCKET')}/historical/"
    DEFAULT_FORMAT = "parquet"
    DEFAULT_MODE = "overwrite"
    DEFAULT_PARTITION_BY = ["partition__year", "partition__month", "partition__day"]

    def __init__(self, spark_client: SparkClient):
        self.spark_client = spark_client

    def load(self, dataframe, name):
        s3_path = os.path.join(self.HISTORICAL_FEATURE_STORE_S3_PATH, name)
        # dataframe = self.create_partitions(dataframe)
        dataframe = verify_column_ts(dataframe)

        self.spark_client.write_table(
            dataframe=dataframe,
            name=name,
            format_=self.DEFAULT_FORMAT,
            mode=self.DEFAULT_MODE,
            partition_by=self.DEFAULT_PARTITION_BY,
            path=s3_path
        )
