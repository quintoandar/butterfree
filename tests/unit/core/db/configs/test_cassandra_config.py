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
