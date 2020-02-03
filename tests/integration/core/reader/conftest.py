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
def kafka_df(sc, spark):
    data = [
        {"key": 1, "value": 400},
        {"key": 2, "value": 800},
        {"key": 3, "value": 1200},
        {"key": 4, "value": 1600},
        {"key": 5, "value": 2000},
        {"key": 6, "value": 2400},
    ]
    return spark.read.json(sc.parallelize(data, 1))


@pytest.fixture()
def target_df_source(sc, spark):
    data = [
        {"id": 1, "feature1": 100, "feature2": 200, "feature3": "400"},
        {"id": 2, "feature1": 200, "feature2": 400, "feature3": "800"},
        {"id": 3, "feature1": 300, "feature2": 600, "feature3": "1200"},
        {"id": 4, "feature1": 400, "feature2": 800, "feature3": "1600"},
        {"id": 5, "feature1": 500, "feature2": 1000, "feature3": "2000"},
        {"id": 6, "feature1": 600, "feature2": 1200, "feature3": "2400"},
    ]
    return spark.read.json(sc.parallelize(data, 1))


@pytest.fixture()
def spark_client_mock():
    return Mock()
