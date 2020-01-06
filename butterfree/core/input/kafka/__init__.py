import os

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, from_json, lit, current_timestamp

from quintoandar_butterfree.core.input.kafka.event_mapping import EventMapping


class KafkaConsumer:
    def __init__(self, event_mapping: EventMapping):
        self._kafka_connection_string = os.environ.get("KAFKA_CONNECTION_STRING")
        self._schema = event_mapping.schema
        self._filter_query = event_mapping.filter_query
        self._options = {
            "kafka.bootstrap.servers": self._kafka_connection_string,
            "subscribe": event_mapping.topic,
        }

    def listen(self, spark: SparkSession) -> DataFrame:
        streamed_dataframe = (
            spark.readStream.format("kafka").options(**self._options).load()
        )
        return self._parse_payload(streamed_dataframe, spark)

    def _parse_payload(self, dataframe: DataFrame, spark: SparkSession) -> DataFrame:
        (
            dataframe.select(col("value").cast("string").alias("json_string"))
            .select(from_json(col("json_string"), self._schema).alias("json"))
            .withColumn("current_timestamp", lit(current_timestamp()))
            .createTempView("dataframe")
        )
        return spark.sql(self._filter_query)
