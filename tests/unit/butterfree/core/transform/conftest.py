import json
from unittest.mock import Mock

from pytest import fixture

from butterfree.core.constants.columns import TIMESTAMP_COLUMN
from butterfree.core.constants.data_type import DataType
from butterfree.core.transform.features import Feature, KeyFeature, TimestampFeature


def make_dataframe(spark_context, spark_session):
    data = [
        {
            "id": 1,
            "ts": "2016-04-11 11:31:11",
            "feature1": 200,
            "feature2": 200,
            "nonfeature": 0,
        },
        {
            "id": 1,
            "ts": "2016-04-11 11:44:12",
            "feature1": 300,
            "feature2": 300,
            "nonfeature": 0,
        },
        {
            "id": 1,
            "ts": "2016-04-11 11:46:24",
            "feature1": 400,
            "feature2": 400,
            "nonfeature": 0,
        },
        {
            "id": 1,
            "ts": "2016-04-11 12:03:21",
            "feature1": 500,
            "feature2": 500,
            "nonfeature": 0,
        },
    ]
    df = spark_session.read.json(spark_context.parallelize(data, 1))
    df = df.withColumn(TIMESTAMP_COLUMN, df.ts.cast(DataType.TIMESTAMP.value))

    return df


def make_rolling_windows_agg_dataframe(spark_context, spark_session):
    data = [
        {
            "id": 1,
            "timestamp": "2016-04-11 00:00:00",
            "feature1__avg_over_1_week_rolling_windows": None,
        },
        {
            "id": 1,
            "timestamp": "2016-04-12 00:00:00",
            "feature1__avg_over_1_week_rolling_windows": 350.0,
        },
        {
            "id": 1,
            "timestamp": "2016-04-13 00:00:00",
            "feature1__avg_over_1_week_rolling_windows": 350.0,
        },
        {
            "id": 1,
            "timestamp": "2016-04-14 00:00:00",
            "feature1__avg_over_1_week_rolling_windows": 350.0,
        },
        {
            "id": 1,
            "timestamp": "2016-04-15 00:00:00",
            "feature1__avg_over_1_week_rolling_windows": 350.0,
        },
        {
            "id": 1,
            "timestamp": "2016-04-16 00:00:00",
            "feature1__avg_over_1_week_rolling_windows": 350.0,
        },
        {
            "id": 1,
            "timestamp": "2016-04-17 00:00:00",
            "feature1__avg_over_1_week_rolling_windows": 350.0,
        },
        {
            "id": 1,
            "timestamp": "2016-04-18 00:00:00",
            "feature1__avg_over_1_week_rolling_windows": 350.0,
        },
        {
            "id": 1,
            "timestamp": "2016-04-19 00:00:00",
            "feature1__avg_over_1_week_rolling_windows": None,
        },
    ]
    df = spark_session.read.json(
        spark_context.parallelize(data).map(lambda x: json.dumps(x))
    )
    df = df.withColumn("timestamp", df.timestamp.cast(DataType.TIMESTAMP.value))

    return df


def make_fs(spark_context, spark_session):
    df = make_dataframe(spark_context, spark_session)
    df = (
        df.withColumn("add", df.feature1 + df.feature2)
        .withColumn("divide", df.feature1 / df.feature2)
        .select("id", TIMESTAMP_COLUMN, "add", "divide")
    )

    return df


@fixture
def dataframe(spark_context, spark_session):
    return make_dataframe(spark_context, spark_session)


@fixture
def feature_set_dataframe(spark_context, spark_session):
    return make_fs(spark_context, spark_session)


@fixture
def rolling_windows_agg_dataframe(spark_context, spark_session):
    return make_rolling_windows_agg_dataframe(spark_context, spark_session)


@fixture
def feature_add(spark_context, spark_session):
    fadd = Mock(spec=Feature)
    fadd.get_output_columns = Mock(return_value=["add"])
    fadd.transform = Mock(return_value=make_fs(spark_context, spark_session))
    return fadd


@fixture
def feature_divide(spark_context, spark_session):
    fdivide = Mock(spec=Feature)
    fdivide.get_output_columns = Mock(return_value=["divide"])
    fdivide.transform = Mock(return_value=make_fs(spark_context, spark_session))
    return fdivide


@fixture
def key_id():
    return KeyFeature(name="id", description="description")


@fixture
def timestamp_c():
    return TimestampFeature()
