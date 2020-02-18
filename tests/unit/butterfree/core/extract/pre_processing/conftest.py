import pytest
from pyspark import SparkContext
from pyspark.sql import session
from pyspark.sql.functions import col, when


@pytest.fixture()
def sc():
    return SparkContext.getOrCreate()


@pytest.fixture()
def spark(sc):
    return session.SparkSession(sc)


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
def target_pivot_df(sc, spark):
    data = [
        {"id": 1, "ts": "2016-04-11 11:31:11", "1": 1, "2": "", "3": "", "4": ""},
        {"id": 1, "ts": "2016-04-11 11:44:12", "1": "", "2": 0, "3": "", "4": ""},
        {"id": 1, "ts": "2016-04-11 11:46:24", "1": "", "2": "", "3": 1, "4": ""},
        {"id": 1, "ts": "2016-04-11 12:03:21", "1": "", "2": "", "3": "", "4": 0},
    ]
    df = spark.read.json(sc.parallelize(data, 1))
    for column in df.columns:
        df = df.withColumn(column, when(col(column) == "", None).otherwise(col(column)))
    return df.orderBy("ts")
