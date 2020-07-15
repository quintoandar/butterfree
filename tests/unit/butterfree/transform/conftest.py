import json
from unittest.mock import Mock

from pytest import fixture

from butterfree.constants import DataType
from butterfree.constants.columns import TIMESTAMP_COLUMN
from butterfree.transform.features import Feature, KeyFeature, TimestampFeature


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
            "feature1": 300,
            "feature2": 400,
            "nonfeature": 0,
        },
        {
            "id": 1,
            "ts": "2016-04-11 12:03:21",
            "feature1": 400,
            "feature2": 500,
            "nonfeature": 0,
        },
        {
            "id": 1,
            "ts": "2016-04-22 12:03:21",
            "feature1": 1000,
            "feature2": 1100,
            "nonfeature": 0,
        },
    ]
    df = spark_session.read.json(spark_context.parallelize(data, 1))
    df = df.withColumn(TIMESTAMP_COLUMN, df.ts.cast(DataType.TIMESTAMP.spark))

    return df


def make_filtering_dataframe(spark_context, spark_session):
    data = [
        {"id": 1, "ts": 1, "feature1": 0, "feature2": None, "feature3": 1},
        {"id": 1, "ts": 2, "feature1": 0, "feature2": 1, "feature3": 1},
        {"id": 1, "ts": 3, "feature1": None, "feature2": None, "feature3": None},
        {"id": 1, "ts": 4, "feature1": 0, "feature2": 1, "feature3": 1},
        {"id": 1, "ts": 5, "feature1": 0, "feature2": 1, "feature3": 1},
        {"id": 1, "ts": 6, "feature1": None, "feature2": None, "feature3": None},
        {"id": 1, "ts": 7, "feature1": None, "feature2": None, "feature3": None},
    ]
    df = spark_session.read.json(
        spark_context.parallelize(data).map(lambda x: json.dumps(x))
    )
    df = df.withColumn(TIMESTAMP_COLUMN, df.ts.cast(DataType.TIMESTAMP.spark))

    return df


def make_output_filtering_dataframe(spark_context, spark_session):
    data = [
        {"id": 1, "ts": 1, "feature1": 0, "feature2": None, "feature3": 1},
        {"id": 1, "ts": 2, "feature1": 0, "feature2": 1, "feature3": 1},
        {"id": 1, "ts": 3, "feature1": None, "feature2": None, "feature3": None},
        {"id": 1, "ts": 4, "feature1": 0, "feature2": 1, "feature3": 1},
        {"id": 1, "ts": 6, "feature1": None, "feature2": None, "feature3": None},
    ]
    df = spark_session.read.json(
        spark_context.parallelize(data).map(lambda x: json.dumps(x))
    )
    df = df.withColumn(TIMESTAMP_COLUMN, df.ts.cast(DataType.TIMESTAMP.spark))

    return df


def make_rolling_windows_agg_dataframe(spark_context, spark_session):
    data = [
        {
            "id": 1,
            "timestamp": "2016-04-11 00:00:00",
            "feature1__avg_over_1_week_rolling_windows": None,
            "feature2__avg_over_1_week_rolling_windows": None,
        },
        {
            "id": 1,
            "timestamp": "2016-04-12 00:00:00",
            "feature1__avg_over_1_week_rolling_windows": 300.0,
            "feature2__avg_over_1_week_rolling_windows": 350.0,
        },
        {
            "id": 1,
            "timestamp": "2016-04-19 00:00:00",
            "feature1__avg_over_1_week_rolling_windows": None,
            "feature2__avg_over_1_week_rolling_windows": None,
        },
        {
            "id": 1,
            "timestamp": "2016-04-23 00:00:00",
            "feature1__avg_over_1_week_rolling_windows": 1000.0,
            "feature2__avg_over_1_week_rolling_windows": 1100.0,
        },
        {
            "id": 1,
            "timestamp": "2016-04-30 00:00:00",
            "feature1__avg_over_1_week_rolling_windows": None,
            "feature2__avg_over_1_week_rolling_windows": None,
        },
    ]
    df = spark_session.read.json(
        spark_context.parallelize(data).map(lambda x: json.dumps(x))
    )
    df = df.withColumn("timestamp", df.timestamp.cast(DataType.TIMESTAMP.spark))

    return df


def make_fs(spark_context, spark_session):
    df = make_dataframe(spark_context, spark_session)
    df = (
        df.withColumn("add", df.feature1 + df.feature2)
        .withColumn("divide", df.feature1 / df.feature2)
        .select("id", TIMESTAMP_COLUMN, "add", "divide")
    )

    return df


def make_fs_dataframe_with_distinct(spark_context, spark_session):
    data = [
        {
            "id": 1,
            "timestamp": "2020-01-01T03:43:49.565+0000",
            "feature": 1,
            "h3": "86a8100efffffff",
        },
        {
            "id": 2,
            "timestamp": "2020-01-01T03:43:49.565+0000",
            "feature": 2,
            "h3": "86a8100efffffff",
        },
        {
            "id": 3,
            "timestamp": "2020-01-02T03:43:49.565+0000",
            "feature": 1,
            "h3": "86a8100efffffff",
        },
        {
            "id": 3,
            "timestamp": "2020-01-02T03:43:49.565+0000",
            "feature": 1,
            "h3": "86a8100efffffff",
        },
        {
            "id": 2,
            "timestamp": "2020-01-02T03:43:49.565+0000",
            "feature": 1,
            "h3": "86a8100efffffff",
        },
    ]
    df = spark_session.read.json(spark_context.parallelize(data, 1))
    df = df.withColumn("timestamp", df.timestamp.cast(DataType.TIMESTAMP.spark))

    return df


def make_target_df_distinct(spark_context, spark_session):
    data = [
        {
            "h3": "86a8100efffffff",
            "timestamp": "2020-01-01T00:00:00.000+0000",
            "feature__sum_over_3_days_rolling_windows": None,
        },
        {
            "h3": "86a8100efffffff",
            "timestamp": "2020-01-02T00:00:00.000+0000",
            "feature__sum_over_3_days_rolling_windows": 3,
        },
        {
            "h3": "86a8100efffffff",
            "timestamp": "2020-01-05T00:00:00.000+0000",
            "feature__sum_over_3_days_rolling_windows": 2,
        },
        {
            "h3": "86a8100efffffff",
            "timestamp": "2020-01-06T00:00:00.000+0000",
            "feature__sum_over_3_days_rolling_windows": None,
        },
    ]
    df = spark_session.read.json(
        spark_context.parallelize(data).map(lambda x: json.dumps(x))
    )
    df = df.withColumn("timestamp", df.timestamp.cast(DataType.TIMESTAMP.spark))

    return df


@fixture
def dataframe(spark_context, spark_session):
    return make_dataframe(spark_context, spark_session)


@fixture
def feature_set_dataframe(spark_context, spark_session):
    return make_fs(spark_context, spark_session)


@fixture
def filtering_dataframe(spark_context, spark_session):
    return make_filtering_dataframe(spark_context, spark_session)


@fixture
def output_filtering_dataframe(spark_context, spark_session):
    return make_output_filtering_dataframe(spark_context, spark_session)


@fixture
def rolling_windows_agg_dataframe(spark_context, spark_session):
    return make_rolling_windows_agg_dataframe(spark_context, spark_session)


@fixture
def feature_set_with_distinct_dataframe(spark_context, spark_session):
    return make_fs_dataframe_with_distinct(spark_context, spark_session)


@fixture
def target_with_distinct_dataframe(spark_context, spark_session):
    return make_target_df_distinct(spark_context, spark_session)


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
def feature1(spark_context, spark_session):
    feature1 = Mock(spec=Feature)
    feature1.get_output_columns = Mock(return_value=["feature1"])
    feature1.transform = Mock(
        return_value=make_filtering_dataframe(spark_context, spark_session)
    )
    return feature1


@fixture
def feature2(spark_context, spark_session):
    feature2 = Mock(spec=Feature)
    feature2.get_output_columns = Mock(return_value=["feature2"])
    feature2.transform = Mock(
        return_value=make_filtering_dataframe(spark_context, spark_session)
    )
    return feature2


@fixture
def feature3(spark_context, spark_session):
    feature3 = Mock(spec=Feature)
    feature3.get_output_columns = Mock(return_value=["feature3"])
    feature3.transform = Mock(
        return_value=make_filtering_dataframe(spark_context, spark_session)
    )
    return feature3


@fixture
def key_id():
    return KeyFeature(name="id", description="description", dtype=DataType.INTEGER)


@fixture
def timestamp_c():
    return TimestampFeature()
