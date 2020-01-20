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
def spark_client():
    return Mock()
