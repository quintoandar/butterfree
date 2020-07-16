from pytest import fixture

from butterfree.configs.db import CassandraConfig, S3Config


@fixture
def cassandra_config(monkeypatch):
    monkeypatch.setenv("CASSANDRA_KEYSPACE", "test")
    monkeypatch.setenv("CASSANDRA_HOST", "test")
    monkeypatch.setenv("CASSANDRA_PASSWORD", "test")
    monkeypatch.setenv("CASSANDRA_USERNAME", "test")

    return CassandraConfig()


@fixture
def s3_config(monkeypatch):
    monkeypatch.setenv("FEATURE_STORE_S3_BUCKET", "test")

    return S3Config()
