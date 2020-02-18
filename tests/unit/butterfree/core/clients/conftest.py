from unittest.mock import Mock

import pytest
from pyspark import SparkContext
from pyspark.sql import session


@pytest.fixture()
def sc():
    return SparkContext.getOrCreate()


@pytest.fixture()
def spark(sc):
    return session.SparkSession(sc)


@pytest.fixture()
def target_df(sc, spark):
    data = [{"col1": "value", "col2": 123}]
    return spark.read.json(sc.parallelize(data, 1))


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
