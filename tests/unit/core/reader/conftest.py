from unittest.mock import Mock

import pytest
from pyspark import SparkContext
from pyspark.sql import session

from butterfree.core.constant.columns import TIMESTAMP_COLUMN


@pytest.fixture()
def sc():
    return SparkContext.getOrCreate()


@pytest.fixture()
def spark(sc):
    return session.SparkSession(sc)


@pytest.fixture()
def column_target_df(sc, spark):
    data = [{"new_col1": "value", "new_col2": 123}]
    return spark.read.json(sc.parallelize(data, 1))


@pytest.fixture()
def target_df(sc, spark):
    data = [{"col1": "value", "col2": 123}]
    return spark.read.json(sc.parallelize(data, 1))


@pytest.fixture()
def spark_client():
    return Mock()


@pytest.fixture
def feature_set_dataframe(sc, spark):
    data = [
        {"id": 1, TIMESTAMP_COLUMN: 0, "feature": 100, "test": "fail"},
        {"id": 2, TIMESTAMP_COLUMN: 0, "feature": 200, "test": "running"},
        {"id": 1, TIMESTAMP_COLUMN: 1, "feature": 110, "test": "pass"},
        {"id": 1, TIMESTAMP_COLUMN: 2, "feature": 120, "test": "pass"},
    ]
    return spark.read.json(sc.parallelize(data, 1))
