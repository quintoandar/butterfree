import os

from pyspark.sql import SparkSession

from quintoandar_butterfree.core.input.datalake import DataLakeEntity


class DataLakeConsumer:
    def __init__(self, spark: SparkSession, *, location: str = None, mode: str = None):
        self._spark = spark
        self._location = (
            location
            or os.getenv("DATALAKE_CLEAN_LOCATION", "s3a://5a-datalake-prod/clean/")
        ).rstrip("/")
        self._mode = mode or "DROPMALFORMED"

    def read_datalake_entity(self, entity: DataLakeEntity):
        self._spark.sql("set spark.sql.caseSensitive=True")
        dataframe = (self.read_from_parquet(entity))
        return dataframe

    def read_from_parquet(self, entity: DataLakeEntity):
        self._spark.conf.set("spark.sql.parquet.binaryAsString", "true")
        return self._spark.read.option("mergeSchema", "True").parquet(
            f"{self._location}/{entity.path}"
        )
