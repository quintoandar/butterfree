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
def pivot_df(sc, spark):
    data = [
        {"id": 1, "ts": "2016-04-11 11:31:11", "pivot_column": 1, "has_feature": 1},
        {"id": 1, "ts": "2016-04-11 11:44:12", "pivot_column": 2, "has_feature": 0},
        {"id": 1, "ts": "2016-04-11 11:46:24", "pivot_column": 3, "has_feature": 1},
        {"id": 1, "ts": "2016-04-11 12:03:21", "pivot_column": 4, "has_feature": 0},
    ]
    df = spark.read.json(sc.parallelize(data, 1))
    return df


@pytest.fixture()
def spark_client():
    return Mock()
