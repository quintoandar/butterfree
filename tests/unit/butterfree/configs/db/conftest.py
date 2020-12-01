from pytest import fixture

from butterfree.configs.db import CassandraConfig, KafkaConfig, MetastoreConfig


@fixture
def cassandra_config(monkeypatch):
    monkeypatch.setenv("CASSANDRA_KEYSPACE", "test")
    monkeypatch.setenv("CASSANDRA_HOST", "test")
    monkeypatch.setenv("CASSANDRA_PASSWORD", "test")
    monkeypatch.setenv("CASSANDRA_USERNAME", "test")

    return CassandraConfig()


@fixture
def kafka_config(monkeypatch):
    monkeypatch.setenv("KAFKA_CONNECTION_STRING", "test")

    return KafkaConfig()


@fixture
def metastore_config(monkeypatch):
    monkeypatch.setenv("FEATURE_STORE_S3_BUCKET", "test")

    return MetastoreConfig()
