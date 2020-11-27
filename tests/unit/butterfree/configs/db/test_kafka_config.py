import pytest

from butterfree.configs.db import KafkaConfig


class TestKafkaConfig:
    def test_mode(self, kafka_config):
        # expecting
        default = "append"
        assert kafka_config.mode == default

        # given
        kafka_config.mode = None
        # then
        assert kafka_config.mode == default

    def test_mode_custom(self, kafka_config):
        # given
        mode = "overwrite"
        kafka_config.mode = mode

        # then
        assert kafka_config.mode == mode

    def test_format(self, kafka_config):
        # expecting
        default = "kafka"
        assert kafka_config.format_ == default

        # given
        kafka_config.format_ = None
        # then
        assert kafka_config.format_ == default

    def test_format_custom(self, kafka_config):
        # given
        format_ = "custom.quintoandar.kafka"
        kafka_config.format_ = format_

        # then
        assert kafka_config.format_ == format_

    def test_kafka_connection_string(self, kafka_config):
        # expecting
        default = "test_host:1234,test_host2:1234"
        assert kafka_config.kafka_connection_string == default

    def test_kafka_connection_string_empty(self, kafka_config, mocker):
        # given
        mocker.patch("butterfree.configs.environment.get_variable", return_value=None)

        with pytest.raises(ValueError, match="cannot be empty"):
            kafka_config.kafka_connection_string = None

    def test_kafka_connection_string_custom(self, kafka_config):
        # given
        kafka_connection_string = "butterfree"
        kafka_config.kafka_connection_string = kafka_connection_string

        # then
        assert kafka_config.kafka_connection_string == kafka_connection_string

    def test_stream_processing_time(self, kafka_config):
        # expecting
        default = "0 seconds"
        assert kafka_config.stream_processing_time == default

    def test_stream_processing_time_custom(self, kafka_config):
        # given
        value = "10 seconds"
        kafka_config.stream_processing_time = value

        # then
        assert kafka_config.stream_processing_time == value

    def test_stream_output_mode(self, kafka_config):
        # expecting
        default = "update"
        assert kafka_config.stream_output_mode == default

    def test_stream_output_mode_custom(self, kafka_config):
        # given
        value = "complete"
        kafka_config.stream_output_mode = value

        # then
        assert kafka_config.stream_output_mode == value

    def test_stream_checkpoint_path(self, kafka_config):
        # expecting
        default = None
        assert kafka_config.stream_checkpoint_path == default

    def test_stream_checkpoint_path_custom(self, kafka_config):
        # given
        value = "s3://path/to/checkpoint"
        kafka_config.stream_checkpoint_path = value

        # then
        assert kafka_config.stream_checkpoint_path == value

    def test_set_credentials_on_instantiation(self):
        kafka_config = KafkaConfig(  # noqa: S106
            kafka_topic="kafka_topic", kafka_connection_string="test_host:1234"
        )
        assert kafka_config.kafka_topic == "kafka_topic"
        assert kafka_config.kafka_connection_string == "test_host:1234"
