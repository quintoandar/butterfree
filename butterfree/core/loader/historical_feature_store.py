from pyspark.sql import DataFrame

from butterfree.core.client.spark_client import SparkClient
from butterfree.core.loader.verify_dataframe import verify_column_ts


class HistoricalFeatureStoreLoader:
    # def __init__(self, dataframe: DataFrame):
    #     self.dataframe = dataframe

    def loader(path, entity, format, partition_column, dataframe: verify_column_ts(DataFrame)):

        s3_path = path + entity

        spark_client_write = SparkClient.write(dataframe)

        spark_client_write.option("path", s3_path).saveAsTable(
            mode="overwrite", format=format, partitionBy=partition_column, name="dataframe_temp")
