import pytest
from pyspark.sql.types import LongType, StringType, StructField, StructType

from butterfree.configs.environment import specification
from butterfree.extract.readers import KafkaReader
from butterfree.testing.dataframe import (
    assert_dataframe_equality,
    create_df_from_collection,
)


class TestKafkaReader:

    RAW_DATA = [
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

    TARGET_DATA = [
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

    @pytest.mark.parametrize(
        "topic, values_schema",
        [("topic", None), (None, StructType([StructField("id", LongType())]))],
    )
    def test_init_invalid_params(self, topic, values_schema):
        # act and assert
        with pytest.raises(ValueError):
            KafkaReader("id", topic, values_schema)

    @pytest.mark.parametrize(
        "topic, topic_options, stream",
        [
            ("topic", None, True),
            ("topic", None, False),
            ("topic", {"startingOffsets": "earliest"}, True),
        ],
    )
    def test_consume(
        self, topic, topic_options, stream, spark_client, spark_context, spark_session
    ):
        """Test for consume method in KafkaReader class.

        The test consists in check the correct use of the read method used inside
        consume. From a kafka format, there are some columns received from the client
        that are in binary. The raw_data and target_data defined in the method are
        used to assert if the consume method is casting the data types correctly,
        besides check if method is been called with the correct args.

        """
        # arrange
        raw_stream_df = create_df_from_collection(
            self.RAW_DATA, spark_context, spark_session
        )
        target_df = create_df_from_collection(
            self.TARGET_DATA, spark_context, spark_session
        )

        spark_client.read.return_value = raw_stream_df
        value_json_schema = StructType(
            [
                StructField("a", LongType()),
                StructField("b", StringType()),
                StructField("c", LongType()),
            ]
        )
        kafka_reader = KafkaReader(
            "test", topic, value_json_schema, topic_options=topic_options, stream=stream
        )

        # act
        output_df = kafka_reader.consume(spark_client)
        connection_string = specification["KAFKA_CONSUMER_CONNECTION_STRING"]
        options = dict(
            {"kafka.bootstrap.servers": connection_string, "subscribe": topic},
            **topic_options if topic_options else {},
        )

        # assert
        spark_client.read.assert_called_once_with(
            format="kafka", options=options, stream=kafka_reader.stream
        )
        assert_dataframe_equality(target_df, output_df)

    def test__struct_df(self, spark_context, spark_session):
        # arrange
        input_df = create_df_from_collection(
            self.RAW_DATA, spark_context, spark_session
        )
        target_df = create_df_from_collection(
            self.TARGET_DATA, spark_context, spark_session
        )
        value_schema = StructType(
            [
                StructField("a", LongType()),
                StructField("b", StringType()),
                StructField("c", LongType()),
            ]
        )
        kafka_reader = KafkaReader("id", "topic", value_schema)

        # act
        output_df = kafka_reader._struct_df(input_df)

        # arrange
        assert_dataframe_equality(target_df, output_df)
