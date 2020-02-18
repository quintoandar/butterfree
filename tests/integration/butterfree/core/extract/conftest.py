from unittest.mock import Mock

import pytest
from pyspark import SparkContext
from pyspark.sql import SparkSession


@pytest.fixture()
def sc():
    return SparkContext.getOrCreate()


@pytest.fixture()
def spark():
    return SparkSession.builder.enableHiveSupport().getOrCreate()


@pytest.fixture()
def target_df_table_reader(sc, spark):
    data = [
        {"id": 1, "feature1": 100},
        {"id": 2, "feature1": 200},
        {"id": 3, "feature1": 300},
        {"id": 4, "feature1": 400},
        {"id": 5, "feature1": 500},
        {"id": 6, "feature1": 600},
    ]
    return spark.read.json(sc.parallelize(data, 1))


@pytest.fixture()
def target_df_source(sc, spark):
    data = [
        {"id": 1, "feature1": 100, "feature2": 200},
        {"id": 2, "feature1": 200, "feature2": 400},
        {"id": 3, "feature1": 300, "feature2": 600},
        {"id": 4, "feature1": 400, "feature2": 800},
        {"id": 5, "feature1": 500, "feature2": 1000},
        {"id": 6, "feature1": 600, "feature2": 1200},
    ]
    return spark.read.json(sc.parallelize(data, 1))


@pytest.fixture()
def spark_client_mock():
    return Mock()
