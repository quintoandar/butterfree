from pytest import fixture

from butterfree.core.db.configs import CassandraWriteConfig


@fixture
def cassandra_config(monkeypatch):
    monkeypatch.setenv("CASSANDRA_KEYSPACE", "test")
    monkeypatch.setenv("CASSANDRA_HOST", "test")
    monkeypatch.setenv("CASSANDRA_PASSWORD", "test")
    monkeypatch.setenv("CASSANDRA_USERNAME", "test")

    return CassandraWriteConfig()
