from unittest.mock import Mock

import pytest
from pyspark.sql.streaming import StreamingQuery

from butterfree.clients import CassandraClient


@pytest.fixture()
def target_df(spark_context, spark_session):
    data = [{"col1": "value", "col2": 123}]
    return spark_session.read.json(spark_context.parallelize(data, 1))


@pytest.fixture()
def mocked_spark_read():
    mock = Mock()
    mock.readStream = mock
    mock.read = mock
    mock.format.return_value = mock
    mock.options.return_value = mock
    return mock


@pytest.fixture()
def mocked_spark_write():
    mock = Mock()
    mock.dataframe = mock
    mock.write = mock
    return mock


@pytest.fixture()
def mocked_stream_df():
    mock = Mock()
    mock.isStreaming = True
    mock.writeStream = mock
    mock.trigger.return_value = mock
    mock.outputMode.return_value = mock
    mock.option.return_value = mock
    mock.foreachBatch.return_value = mock
    mock.start.return_value = Mock(spec=StreamingQuery)
    return mock


@pytest.fixture
def cassandra_client():
    return CassandraClient(
        cassandra_host=["mock"], cassandra_key_space="dummy_keyspace"
    )


@pytest.fixture
def cassandra_feature_set():
    return [
        {"feature1": "value1", "feature2": 10.5},
        {"feature1": "value1", "feature2": 10},
    ]
