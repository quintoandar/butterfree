import pytest
from unittest.mock import Mock
from pyspark.sql import session
from pyspark import SparkContext


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
def spark_client():
    return Mock()
