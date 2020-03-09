"""Struct kafka soruce transform for dataframes."""
from pyspark.sql.dataframe import DataFrame, StructType
from pyspark.sql.functions import struct

from butterfree.core.extract.pre_processing.explode_json_column import (
    explode_json_column,
)

KAFKA_COLUMNS = [
    "key",
    "topic",
    "value",
    "partition",
    "offset",
    "timestamp",
    "timestampType",
]


def struct_kafka_source(df: DataFrame, value_schema: StructType) -> DataFrame:
    """Struct a dataframe coming directly from Kafka source.

    Under the default "value" column comingo from Kafka there is the custom
    fields created by some producer. This functions will struct the dataframe as
    to get all desired fields from "value" and organize all Kafka default columns
    under "kafka_metadata" column. It is important to notice that the declared
    value_schema can suffer from the same effects described in explode_json_column
    method.

    Args:
        df: direct dataframe output from from KafkaReader.
        value_schema: schema of the default column named "value" from Kafka.

    Returns:
        Structured dataframe with kafka value fields as columns.
            All other default fields from Kafka will be stored under "kafka_metadata"
            column.

    """
    df = df.withColumn("kafka_metadata", struct(*KAFKA_COLUMNS))
    df = explode_json_column(df, column="value", json_schema=value_schema)
    return df.select([field.name for field in value_schema] + ["kafka_metadata"])
