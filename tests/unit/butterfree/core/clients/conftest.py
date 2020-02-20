from unittest.mock import Mock

import pytest


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
