from pytest import fixture

from butterfree.configs.db import CassandraConfig, KafkaConfig, S3Config


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
def s3_config(monkeypatch):
    monkeypatch.setenv("FEATURE_STORE_S3_BUCKET", "test")

    return S3Config()
