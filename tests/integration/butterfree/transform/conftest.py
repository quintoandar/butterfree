import json

from pytest import fixture

from butterfree.constants import DataType
from butterfree.constants.columns import TIMESTAMP_COLUMN


@fixture
def feature_set_dataframe(spark_context, spark_session):
    data = [
        {
            "id": 1,
            "origin_ts": "2016-04-11 11:31:11",
            "fixed_ts": "2016-04-11 12:03:21",
            "feature1": 200,
            "feature2": 200,
        },
        {
            "id": 1,
            "origin_ts": "2016-04-11 11:44:12",
            "fixed_ts": "2016-04-11 12:03:21",
            "feature1": 300,
            "feature2": 300,
        },
        {
            "id": 1,
            "origin_ts": "2016-04-11 11:46:24",
            "fixed_ts": "2016-04-11 12:03:21",
            "feature1": 400,
            "feature2": 400,
        },
        {
            "id": 1,
            "origin_ts": "2016-04-11 12:03:21",
            "fixed_ts": "2016-04-11 12:03:21",
            "feature1": 500,
            "feature2": 500,
        },
    ]
    df = spark_session.read.json(spark_context.parallelize(data, 1))
    df = df.withColumn(TIMESTAMP_COLUMN, df.origin_ts.cast(DataType.TIMESTAMP.spark))
    df = df.withColumn("fixed_ts", df.fixed_ts.cast(DataType.TIMESTAMP.spark))

    return df


@fixture
def feature_set_df_pivot(spark_context, spark_session):
    data = [
        {
            "id": 1,
            "timestamp": "2016-04-11 11:31:11",
            "fixed_ts": "2016-04-11 12:03:21",
            "feature1": 200,
            "pivot_col": "N",
        },
        {
            "id": 1,
            "timestamp": "2016-04-11 11:44:12",
            "fixed_ts": "2016-04-11 12:03:21",
            "feature1": 300,
            "pivot_col": "N",
        },
        {
            "id": 1,
            "timestamp": "2016-04-11 11:46:24",
            "fixed_ts": "2016-04-11 12:03:21",
            "feature1": 400,
            "pivot_col": "S",
        },
        {
            "id": 1,
            "timestamp": "2016-04-11 12:03:21",
            "fixed_ts": "2016-04-11 12:03:21",
            "feature1": 500,
            "pivot_col": "S",
        },
    ]
    df = spark_session.read.json(spark_context.parallelize(data, 1))
    df = df.withColumn(TIMESTAMP_COLUMN, df.timestamp.cast(DataType.TIMESTAMP.spark))
    df = df.withColumn("fixed_ts", df.fixed_ts.cast(DataType.TIMESTAMP.spark))

    return df


@fixture
def target_df_pivot_agg(spark_context, spark_session):
    data = [
        {
            "id": 1,
            "timestamp": "2016-04-11 12:03:21",
            "S_feature__avg": 450,
            "S_feature__stddev_pop": 50,
            "N_feature__avg": 250,
            "N_feature__stddev_pop": 50,
        },
    ]
    df = spark_session.read.json(spark_context.parallelize(data, 1))
    df = df.withColumn(TIMESTAMP_COLUMN, df.timestamp.cast(DataType.TIMESTAMP.spark))

    return df


@fixture
def h3_input_df(spark_context, spark_session):
    data = [
        {
            "id": 1,
            "origin_ts": "2016-04-11 11:31:11",
            "feature1": 200,
            "feature2": 200,
            "lat": -23.554190,
            "lng": -46.670723,
            "house_id": 8921,
        },
        {
            "id": 1,
            "origin_ts": "2016-04-11 11:44:12",
            "feature1": 300,
            "feature2": 300,
            "lat": -23.554190,
            "lng": -46.670723,
            "house_id": 8921,
        },
    ]
    df = spark_session.read.json(spark_context.parallelize(data, 1))
    df = df.withColumn(TIMESTAMP_COLUMN, df.origin_ts.cast(DataType.TIMESTAMP.spark))

    return df


@fixture
def h3_target_df(spark_context, spark_session):
    data = [
        {
            "h3_id": "8ba8100ea0d5fff",
            "timestamp": "2016-04-11 00:00:00",
            "house_id__count_over_1_day_rolling_windows": None,
        },
        {
            "h3_id": "88a8100ea1fffff",
            "timestamp": "2016-04-11 00:00:00",
            "house_id__count_over_1_day_rolling_windows": None,
        },
        {
            "h3_id": "8ca8100ea0d57ff",
            "timestamp": "2016-04-11 00:00:00",
            "house_id__count_over_1_day_rolling_windows": None,
        },
        {
            "h3_id": "89a8100ea0fffff",
            "timestamp": "2016-04-11 00:00:00",
            "house_id__count_over_1_day_rolling_windows": None,
        },
        {
            "h3_id": "86a8100efffffff",
            "timestamp": "2016-04-11 00:00:00",
            "house_id__count_over_1_day_rolling_windows": None,
        },
        {
            "h3_id": "87a8100eaffffff",
            "timestamp": "2016-04-11 00:00:00",
            "house_id__count_over_1_day_rolling_windows": None,
        },
        {
            "h3_id": "8aa8100ea0d7fff",
            "timestamp": "2016-04-11 00:00:00",
            "house_id__count_over_1_day_rolling_windows": None,
        },
        {
            "h3_id": "8ba8100ea0d5fff",
            "timestamp": "2016-04-12 00:00:00",
            "house_id__count_over_1_day_rolling_windows": 2,
        },
        {
            "h3_id": "88a8100ea1fffff",
            "timestamp": "2016-04-12 00:00:00",
            "house_id__count_over_1_day_rolling_windows": 2,
        },
        {
            "h3_id": "8ca8100ea0d57ff",
            "timestamp": "2016-04-12 00:00:00",
            "house_id__count_over_1_day_rolling_windows": 2,
        },
        {
            "h3_id": "89a8100ea0fffff",
            "timestamp": "2016-04-12 00:00:00",
            "house_id__count_over_1_day_rolling_windows": 2,
        },
        {
            "h3_id": "86a8100efffffff",
            "timestamp": "2016-04-12 00:00:00",
            "house_id__count_over_1_day_rolling_windows": 2,
        },
        {
            "h3_id": "87a8100eaffffff",
            "timestamp": "2016-04-12 00:00:00",
            "house_id__count_over_1_day_rolling_windows": 2,
        },
        {
            "h3_id": "8aa8100ea0d7fff",
            "timestamp": "2016-04-12 00:00:00",
            "house_id__count_over_1_day_rolling_windows": 2,
        },
        {
            "h3_id": "8ba8100ea0d5fff",
            "timestamp": "2016-04-13 00:00:00",
            "house_id__count_over_1_day_rolling_windows": None,
        },
        {
            "h3_id": "88a8100ea1fffff",
            "timestamp": "2016-04-13 00:00:00",
            "house_id__count_over_1_day_rolling_windows": None,
        },
        {
            "h3_id": "8ca8100ea0d57ff",
            "timestamp": "2016-04-13 00:00:00",
            "house_id__count_over_1_day_rolling_windows": None,
        },
        {
            "h3_id": "89a8100ea0fffff",
            "timestamp": "2016-04-13 00:00:00",
            "house_id__count_over_1_day_rolling_windows": None,
        },
        {
            "h3_id": "86a8100efffffff",
            "timestamp": "2016-04-13 00:00:00",
            "house_id__count_over_1_day_rolling_windows": None,
        },
        {
            "h3_id": "87a8100eaffffff",
            "timestamp": "2016-04-13 00:00:00",
            "house_id__count_over_1_day_rolling_windows": None,
        },
        {
            "h3_id": "8aa8100ea0d7fff",
            "timestamp": "2016-04-13 00:00:00",
            "house_id__count_over_1_day_rolling_windows": None,
        },
    ]
    df = spark_session.read.json(
        spark_context.parallelize(data).map(lambda x: json.dumps(x))
    )
    df = df.withColumn(TIMESTAMP_COLUMN, df.timestamp.cast(DataType.TIMESTAMP.spark))

    return df


@fixture
def target_df_without_window(spark_context, spark_session):
    data = [
        {
            "id": 1,
            "timestamp": "2016-04-11 12:03:21",
            "feature1__avg": 350,
            "feature2__count": 4,
        },
    ]
    df = spark_session.read.json(spark_context.parallelize(data, 1))
    df = df.withColumn(TIMESTAMP_COLUMN, df.timestamp.cast(DataType.TIMESTAMP.spark))

    return df


@fixture
def fixed_windows_output_feature_set_dataframe(spark_context, spark_session):
    data = [
        {
            "id": 1,
            "origin_ts": "2016-04-11 11:31:11",
            "feature1__avg_over_2_minutes_fixed_windows": 200,
            "feature1__avg_over_15_minutes_fixed_windows": 200,
            "feature1__stddev_pop_over_2_minutes_fixed_windows": 0,
            "feature1__stddev_pop_over_15_minutes_fixed_windows": 0,
            "divided_feature": 1,
        },
        {
            "id": 1,
            "origin_ts": "2016-04-11 11:44:12",
            "feature1__avg_over_2_minutes_fixed_windows": 300,
            "feature1__avg_over_15_minutes_fixed_windows": 250,
            "feature1__stddev_pop_over_2_minutes_fixed_windows": 0,
            "feature1__stddev_pop_over_15_minutes_fixed_windows": 50,
            "divided_feature": 1,
        },
        {
            "id": 1,
            "origin_ts": "2016-04-11 11:46:24",
            "feature1__avg_over_2_minutes_fixed_windows": 400,
            "feature1__avg_over_15_minutes_fixed_windows": 350,
            "feature1__stddev_pop_over_2_minutes_fixed_windows": 0,
            "feature1__stddev_pop_over_15_minutes_fixed_windows": 50,
            "divided_feature": 1,
        },
        {
            "id": 1,
            "origin_ts": "2016-04-11 12:03:21",
            "feature1__avg_over_2_minutes_fixed_windows": 500,
            "feature1__avg_over_15_minutes_fixed_windows": 500,
            "feature1__stddev_pop_over_2_minutes_fixed_windows": 0,
            "feature1__stddev_pop_over_15_minutes_fixed_windows": 0,
            "divided_feature": 1,
        },
    ]
    df = spark_session.read.json(
        spark_context.parallelize(data).map(lambda x: json.dumps(x))
    )
    df = df.withColumn(TIMESTAMP_COLUMN, df.origin_ts.cast(DataType.TIMESTAMP.spark))

    return df


@fixture
def rolling_windows_output_feature_set_dataframe(spark_context, spark_session):
    data = [
        {
            "id": 1,
            "origin_ts": "2016-04-12 00:00:00",
            "feature1__avg_over_1_day_rolling_windows": 350.0,
            "feature1__avg_over_1_week_rolling_windows": 350.0,
            "feature1__stddev_pop_over_1_day_rolling_windows": 111.80339887498948,
            "feature1__stddev_pop_over_1_week_rolling_windows": 111.80339887498948,
        },
        {
            "id": 1,
            "origin_ts": "2016-04-13 00:00:00",
            "feature1__avg_over_1_day_rolling_windows": None,
            "feature1__avg_over_1_week_rolling_windows": 350.0,
            "feature1__stddev_pop_over_1_day_rolling_windows": None,
            "feature1__stddev_pop_over_1_week_rolling_windows": 111.80339887498948,
        },
        {
            "id": 1,
            "origin_ts": "2016-04-19 00:00:00",
            "feature1__avg_over_1_day_rolling_windows": None,
            "feature1__avg_over_1_week_rolling_windows": None,
            "feature1__stddev_pop_over_1_day_rolling_windows": None,
            "feature1__stddev_pop_over_1_week_rolling_windows": None,
        },
    ]
    df = spark_session.read.json(
        spark_context.parallelize(data).map(lambda x: json.dumps(x))
    )
    df = df.withColumn(TIMESTAMP_COLUMN, df.origin_ts.cast(DataType.TIMESTAMP.spark))

    return df


@fixture
def rolling_windows_output_feature_set_dataframe_base_date(
    spark_context, spark_session
):
    data = [
        {
            "id": 1,
            "origin_ts": "2016-04-11 00:00:00",
            "feature1__avg_over_1_day_rolling_windows": None,
            "feature2__avg_over_1_day_rolling_windows": None,
            "feature1__avg_over_1_week_rolling_windows": None,
            "feature2__avg_over_1_week_rolling_windows": None,
            "feature1__stddev_pop_over_1_day_rolling_windows": None,
            "feature2__stddev_pop_over_1_day_rolling_windows": None,
            "feature1__stddev_pop_over_1_week_rolling_windows": None,
            "feature2__stddev_pop_over_1_week_rolling_windows": None,
        },
        {
            "id": 1,
            "origin_ts": "2016-04-12 00:00:00",
            "feature1__avg_over_1_day_rolling_windows": 350.0,
            "feature2__avg_over_1_day_rolling_windows": 350.0,
            "feature1__avg_over_1_week_rolling_windows": 350.0,
            "feature2__avg_over_1_week_rolling_windows": 350.0,
            "feature1__stddev_pop_over_1_day_rolling_windows": 111.80339887498948,
            "feature2__stddev_pop_over_1_day_rolling_windows": 111.80339887498948,
            "feature1__stddev_pop_over_1_week_rolling_windows": 111.80339887498948,
            "feature2__stddev_pop_over_1_week_rolling_windows": 111.80339887498948,
        },
        {
            "id": 1,
            "origin_ts": "2016-04-13 00:00:00",
            "feature1__avg_over_1_day_rolling_windows": None,
            "feature2__avg_over_1_day_rolling_windows": None,
            "feature1__avg_over_1_week_rolling_windows": 350.0,
            "feature2__avg_over_1_week_rolling_windows": 350.0,
            "feature1__stddev_pop_over_1_day_rolling_windows": None,
            "feature2__stddev_pop_over_1_day_rolling_windows": None,
            "feature1__stddev_pop_over_1_week_rolling_windows": 111.80339887498948,
            "feature2__stddev_pop_over_1_week_rolling_windows": 111.80339887498948,
        },
    ]
    df = spark_session.read.json(
        spark_context.parallelize(data).map(lambda x: json.dumps(x))
    )
    df = df.withColumn(TIMESTAMP_COLUMN, df.origin_ts.cast(DataType.TIMESTAMP.spark))

    return df
