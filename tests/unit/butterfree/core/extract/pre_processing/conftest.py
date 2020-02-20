import json
from typing import List

import pytest
from pyspark import SparkContext
from pyspark.sql import DataFrame, session


@pytest.fixture()
def sc():
    return SparkContext.getOrCreate()


@pytest.fixture()
def spark(sc):
    return (
        session.SparkSession(sc)
        .builder.config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )


@pytest.fixture()
def input_df(sc, spark):
    data = [
        {"id": 1, "ts": "2016-04-11 11:31:11", "pivot_column": 1, "has_feature": 1},
        {"id": 1, "ts": "2016-04-11 11:44:12", "pivot_column": 2, "has_feature": 0},
        {"id": 1, "ts": "2016-04-11 11:46:24", "pivot_column": 3, "has_feature": 1},
        {"id": 1, "ts": "2016-04-11 12:03:21", "pivot_column": 4, "has_feature": 0},
        {"id": 1, "ts": "2016-04-11 13:46:24", "pivot_column": 3, "has_feature": None},
    ]
    df = spark.read.json(sc.parallelize(data).map(lambda x: json.dumps(x)))
    return df


@pytest.fixture()
def pivot_df(sc, spark):
    data = [
        {"id": 1, "ts": "2016-04-11 11:31:11", "1": 1, "2": None, "3": None, "4": None},
        {"id": 1, "ts": "2016-04-11 11:44:12", "1": None, "2": 0, "3": None, "4": None},
        {"id": 1, "ts": "2016-04-11 11:46:24", "1": None, "2": None, "3": 1, "4": None},
        {"id": 1, "ts": "2016-04-11 12:03:21", "1": None, "2": None, "3": None, "4": 0},
        {
            "id": 1,
            "ts": "2016-04-11 13:46:24",
            "1": None,
            "2": None,
            "3": None,
            "4": None,
        },
    ]
    df = spark.read.json(sc.parallelize(data).map(lambda x: json.dumps(x)))
    return df.orderBy("ts")


@pytest.fixture()
def pivot_ffill_df(sc, spark):
    data = [
        {"id": 1, "ts": "2016-04-11 11:31:11", "1": 1, "2": None, "3": None, "4": None},
        {"id": 1, "ts": "2016-04-11 11:44:12", "1": 1, "2": 0, "3": None, "4": None},
        {"id": 1, "ts": "2016-04-11 11:46:24", "1": 1, "2": 0, "3": 1, "4": None},
        {"id": 1, "ts": "2016-04-11 12:03:21", "1": 1, "2": 0, "3": 1, "4": 0},
        {"id": 1, "ts": "2016-04-11 13:46:24", "1": 1, "2": 0, "3": 1, "4": 0},
    ]
    df = spark.read.json(sc.parallelize(data).map(lambda x: json.dumps(x)))
    return df.orderBy("ts")


@pytest.fixture()
def pivot_ffill_mock_df(sc, spark):
    data = [
        {"id": 1, "ts": "2016-04-11 11:31:11", "1": 1, "2": None, "3": None, "4": None},
        {"id": 1, "ts": "2016-04-11 11:44:12", "1": 1, "2": 0, "3": None, "4": None},
        {"id": 1, "ts": "2016-04-11 11:46:24", "1": 1, "2": 0, "3": 1, "4": None},
        {"id": 1, "ts": "2016-04-11 12:03:21", "1": 1, "2": 0, "3": 1, "4": 0},
        {"id": 1, "ts": "2016-04-11 13:46:24", "1": 1, "2": 0, "3": None, "4": 0},
    ]
    df = spark.read.json(sc.parallelize(data).map(lambda x: json.dumps(x)))
    return df.orderBy("ts")


def compare_dataframes(
    actual_df: DataFrame, expected_df: DataFrame, columns_sort: List[str] = None
):
    if not columns_sort:
        columns_sort = actual_df.schema.fieldNames()
    return sorted(actual_df.select(*columns_sort).collect()) == sorted(
        expected_df.select(*columns_sort).collect()
    )
