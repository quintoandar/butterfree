import pytest


class TestCassandraWriteConfig:
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
        mocker.patch(
            "butterfree.core.configs.environment.get_variable", return_value=None
        )

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
        mocker.patch(
            "butterfree.core.configs.environment.get_variable", return_value=None
        )

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
        mocker.patch(
            "butterfree.core.configs.environment.get_variable", return_value=None
        )

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
        mocker.patch(
            "butterfree.core.configs.environment.get_variable", return_value=None
        )

        with pytest.raises(ValueError, match="cannot be empty"):
            cassandra_config.password = None

    def test_password_custom(self, cassandra_config):
        # given
        value = "butterfree"
        cassandra_config.keyspace = value

        # then
        assert cassandra_config.keyspace == value
