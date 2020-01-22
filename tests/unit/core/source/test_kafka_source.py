import pytest

from butterfree.core.source import KafkaSource


class TestKafkaSource:
    @pytest.mark.parametrize(
        "connection_string, topic",
        [(None, "topic"), ("host1:port,host2:port", 123), (123, None)],
    )
    def test_init_invalid_params(self, connection_string, topic, spark_client):
        # act and assert
        with pytest.raises(ValueError):
            KafkaSource("id", spark_client, connection_string, topic)

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
        # arrange
        raw_data = [
            {"key": 10101111, "value": 10101111}
        ]  # data gets in binary to spark
        target_data = [{"key": "10101111", "value": "10101111"}]
        raw_stream_df = spark.read.json(sc.parallelize(raw_data, 1))
        target_df = spark.read.json(sc.parallelize(target_data, 1))

        spark_client.read.return_value = raw_stream_df
        kafka_source = KafkaSource(
            "test", spark_client, connection_string, topic, topic_options, stream
        )

        # act
        output_df = kafka_source.consume()
        options = dict(
            {"kafka.bootstrap.servers": connection_string, "subscribe": topic},
            **topic_options if topic_options else {}
        )

        # assert
        spark_client.read.assert_called_once_with(
            format="kafka", options=options, stream=kafka_source.stream
        )
        assert target_df.collect() == output_df.collect()
