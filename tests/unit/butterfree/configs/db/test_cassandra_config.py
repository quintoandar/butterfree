import pytest

from butterfree.configs.db import CassandraConfig


class TestCassandraConfig:
    def test_mode(self, cassandra_config):
        # expecting
        default = "append"
        assert cassandra_config.mode == default

        # given
        cassandra_config.mode = None
        # then
        assert cassandra_config.mode == default

    def test_mode_custom(self, cassandra_config):
        # given
        mode = "overwrite"
        cassandra_config.mode = mode

        # then
        assert cassandra_config.mode == mode

    def test_format(self, cassandra_config):
        # expecting
        default = "org.apache.spark.sql.cassandra"
        assert cassandra_config.format_ == default

        # given
        cassandra_config.format_ = None
        # then
        assert cassandra_config.format_ == default

    def test_format_custom(self, cassandra_config):
        # given
        format_ = "custom.quintoandar.cassandra"
        cassandra_config.format_ = format_

        # then
        assert cassandra_config.format_ == format_

    def test_keyspace(self, cassandra_config):
        # expecting
        default = "test"
        assert cassandra_config.keyspace == default

    def test_keyspace_empty(self, cassandra_config, mocker):
        # given
        mocker.patch("butterfree.configs.environment.get_variable", return_value=None)

        with pytest.raises(ValueError, match="cannot be empty"):
            cassandra_config.keyspace = None

    def test_keyspace_custom(self, cassandra_config):
        # given
        keyspace = "butterfree"
        cassandra_config.keyspace = keyspace

        # then
        assert cassandra_config.keyspace == keyspace

    def test_host(self, cassandra_config):
        # expecting
        default = "test"
        assert cassandra_config.host == default

    def test_host_empty(self, cassandra_config, mocker):
        # given
        mocker.patch("butterfree.configs.environment.get_variable", return_value=None)

        with pytest.raises(ValueError, match="cannot be empty"):
            cassandra_config.host = None

    def test_host_custom(self, cassandra_config):
        # given
        host = "butterfree"
        cassandra_config.keyspace = host

        # then
        assert cassandra_config.keyspace == host

    def test_username(self, cassandra_config):
        # expecting
        default = "test"
        assert cassandra_config.username == default

    def test_username_empty(self, cassandra_config, mocker):
        # given
        mocker.patch("butterfree.configs.environment.get_variable", return_value=None)

        with pytest.raises(ValueError, match="cannot be empty"):
            cassandra_config.username = None

    def test_username_custom(self, cassandra_config):
        # given
        username = "butterfree"
        cassandra_config.keyspace = username

        # then
        assert cassandra_config.keyspace == username

    def test_password(self, cassandra_config):
        # expecting
        default = "test"
        assert cassandra_config.password == default

    def test_password_empty(self, cassandra_config, mocker):
        # given
        mocker.patch("butterfree.configs.environment.get_variable", return_value=None)

        with pytest.raises(ValueError, match="cannot be empty"):
            cassandra_config.password = None

    def test_password_custom(self, cassandra_config):
        # given
        value = "butterfree"
        cassandra_config.keyspace = value

        # then
        assert cassandra_config.keyspace == value

    def test_stream_processing_time(self, cassandra_config):
        # expecting
        default = "0 seconds"
        assert cassandra_config.stream_processing_time == default

    def test_stream_processing_time_custom(self, cassandra_config):
        # given
        value = "10 seconds"
        cassandra_config.stream_processing_time = value

        # then
        assert cassandra_config.stream_processing_time == value

    def test_stream_output_mode(self, cassandra_config):
        # expecting
        default = "update"
        assert cassandra_config.stream_output_mode == default

    def test_stream_output_mode_custom(self, cassandra_config):
        # given
        value = "complete"
        cassandra_config.stream_output_mode = value

        # then
        assert cassandra_config.stream_output_mode == value

    def test_stream_checkpoint_path(self, cassandra_config):
        # expecting
        default = None
        assert cassandra_config.stream_checkpoint_path == default

    def test_stream_checkpoint_path_custom(self, cassandra_config):
        # given
        value = "s3://path/to/checkpoint"
        cassandra_config.stream_checkpoint_path = value

        # then
        assert cassandra_config.stream_checkpoint_path == value

    def test_set_credentials_on_instantiation(self):
        cassandra_config = CassandraConfig(  # noqa: S106
            username="username", password="password", host="host", keyspace="keyspace"
        )
        assert cassandra_config.username == "username"
        assert cassandra_config.password == "password"
        assert cassandra_config.host == "host"
        assert cassandra_config.keyspace == "keyspace"
