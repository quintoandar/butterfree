import pytest

from butterfree.core.extract.readers import KafkaReader


class TestKafkaReader:
    @pytest.mark.parametrize(
        "connection_string, topic",
        [(None, "topic"), ("host1:port,host2:port", 123), (123, None)],
    )
    def test_init_invalid_params(self, connection_string, topic):
        # act and assert
        with pytest.raises(ValueError):
            KafkaReader("id", connection_string, topic)

    @pytest.mark.parametrize(
        "connection_string, topic, topic_options, stream",
        [
            ("host1:port,host2:port", "topic", None, True),
            ("host1:port,host2:port", "topic", None, False),
            ("host1:port,host2:port", "topic", {"startingOffsets": "earliest"}, True),
        ],
    )
    def test_consume(
        self, connection_string, topic, topic_options, stream, spark_client, spark, sc,
    ):
        """Test for consume method in KafkaReader class.

        The test consists in check the correct use of the read method used inside
        consume. From a kafka format, there are some columns received from the client
        that are in binary. The raw_data and target_data defined in the method are
        used to assert if the consume method is casting the data types correctly,
        besides check if method is been called with the correct args.
        """
        # arrange
        raw_data = [
            {"key": 10101111, "value": 10101111}
        ]  # data gets in binary to spark
        target_data = [{"key": "10101111", "value": "10101111"}]
        raw_stream_df = spark.read.json(sc.parallelize(raw_data, 1))
        target_df = spark.read.json(sc.parallelize(target_data, 1))

        spark_client.read.return_value = raw_stream_df
        kafka_reader = KafkaReader(
            "test", connection_string, topic, topic_options, stream
        )

        # act
        output_df = kafka_reader.consume(spark_client)
        options = dict(
            {"kafka.bootstrap.servers": connection_string, "subscribe": topic},
            **topic_options if topic_options else {},
        )

        # assert
        spark_client.read.assert_called_once_with(
            format="kafka", options=options, stream=kafka_reader.stream
        )
        assert target_df.collect() == output_df.collect()
