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
def mocked_df(sc, spark):
    data = [
        {"id": 1, "ts": "2016-04-11 11:31:11", "feature1": 200, "feature2": 200},
        {"id": 1, "ts": "2016-04-11 11:44:12", "feature1": 300, "feature2": 300},
        {"id": 1, "ts": "2016-04-11 11:46:24", "feature1": 400, "feature2": 400},
        {"id": 1, "ts": "2016-04-11 12:03:21", "feature1": 500, "feature2": 500},
    ]
    df = spark.read.json(sc.parallelize(data, 1))
    df = df.withColumn("timestamp", df.ts.cast("timestamp"))

    return df
