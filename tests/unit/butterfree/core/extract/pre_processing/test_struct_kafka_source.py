from pyspark.sql.types import IntegerType, StringType, StructField, StructType

from butterfree.core.extract.pre_processing import struct_kafka_source
from butterfree.testing.dataframe import (
    assert_dataframe_equality,
    create_df_from_collection,
)


def test_struct_kafka_transform(spark_context, spark_session):
    # arrange
    input_data = [
        {
            "key": "123",
            "value": '{"a": 123, "b": "abc", "c": "123"}',
            "topic": "topic",
            "partition": 1,
            "offset": 1,
            "timestamp": "2020-03-09T12:00:00+00:00",
            "timestampType": 1,
        }
    ]
    target_data = [
        {
            "kafka_metadata": {
                "key": "123",
                "value": '{"a": 123, "b": "abc", "c": "123"}',
                "topic": "topic",
                "partition": 1,
                "offset": 1,
                "timestamp": "2020-03-09T12:00:00+00:00",
                "timestampType": 1,
            },
            "a": 123,
            "b": "abc",
            "c": 123,
        }
    ]

    input_df = create_df_from_collection(input_data, spark_context, spark_session)
    target_df = create_df_from_collection(target_data, spark_context, spark_session)

    value_schema = StructType(
        [
            StructField("a", IntegerType()),
            StructField("b", StringType()),
            StructField("c", IntegerType()),
        ]
    )

    # act
    output_df = struct_kafka_source(input_df, value_schema)

    # arrange
    assert_dataframe_equality(target_df, output_df)
