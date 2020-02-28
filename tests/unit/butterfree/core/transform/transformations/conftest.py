import json

from pytest import fixture

from butterfree.core.constants.columns import TIMESTAMP_COLUMN
from butterfree.core.constants.data_type import DataType


@fixture
def h3_df(spark_context, spark_session):
    data = [
        {"id": 1, "feature": 200, "lat": -23.554190, "lng": -46.670723},
        {"id": 1, "feature": 300, "lat": -23.554190, "lng": -46.670723},
        {"id": 1, "feature": 400, "lat": -23.554190, "lng": -46.670723},
        {"id": 1, "feature": 500, "lat": -23.554190, "lng": -46.670723},
    ]
    df = spark_session.read.json(
        spark_context.parallelize(data).map(lambda x: json.dumps(x))
    )

    return df


@fixture
def h3_target_df(spark_context, spark_session):
    data = [
        {
            "id": 1,
            "feature": 200,
            "lat": -23.554190,
            "lng": -46.670723,
            "lat_lng__h3_hash__6": "86a8100efffffff",
            "lat_lng__h3_hash__7": "87a8100eaffffff",
            "lat_lng__h3_hash__8": "88a8100ea1fffff",
        },
        {
            "id": 1,
            "feature": 300,
            "lat": -23.554190,
            "lng": -46.670723,
            "lat_lng__h3_hash__6": "86a8100efffffff",
            "lat_lng__h3_hash__7": "87a8100eaffffff",
            "lat_lng__h3_hash__8": "88a8100ea1fffff",
        },
        {
            "id": 1,
            "feature": 400,
            "lat": -23.554190,
            "lng": -46.670723,
            "lat_lng__h3_hash__6": "86a8100efffffff",
            "lat_lng__h3_hash__7": "87a8100eaffffff",
            "lat_lng__h3_hash__8": "88a8100ea1fffff",
        },
        {
            "id": 1,
            "feature": 500,
            "lat": -23.554190,
            "lng": -46.670723,
            "lat_lng__h3_hash__6": "86a8100efffffff",
            "lat_lng__h3_hash__7": "87a8100eaffffff",
            "lat_lng__h3_hash__8": "88a8100ea1fffff",
        },
    ]
    df = spark_session.read.json(
        spark_context.parallelize(data).map(lambda x: json.dumps(x))
    )

    return df


@fixture
def custom_target_df(spark_context, spark_session):
    data = [
        {
            "id": 1,
            TIMESTAMP_COLUMN: 1460374271,
            "feature1": 100,
            "feature2": 100,
            "feature": 1.0,
        },
        {
            "id": 2,
            TIMESTAMP_COLUMN: 1460375052,
            "feature1": 200,
            "feature2": 200,
            "feature": 1.0,
        },
        {
            "id": 3,
            TIMESTAMP_COLUMN: 1460375184,
            "feature1": 300,
            "feature2": 300,
            "feature": 1.0,
        },
        {
            "id": 4,
            TIMESTAMP_COLUMN: 1460376201,
            "feature1": 400,
            "feature2": 400,
            "feature": 1.0,
        },
    ]
    df = spark_session.read.json(
        spark_context.parallelize(data).map(lambda x: json.dumps(x))
    )
    df = df.withColumn(TIMESTAMP_COLUMN, df.timestamp.cast(DataType.TIMESTAMP.value))
    return df


@fixture
def fixed_windows_target_df(spark_context, spark_session):
    data = [
        {
            "id": 1,
            TIMESTAMP_COLUMN: "2016-04-11 11:31:11",
            "feature1": 100,
            "feature2": 100,
            "feature1__avg_over_2_minutes_fixed_windows": 100.0,
            "feature1__avg_over_15_minutes_fixed_windows": 100.0,
            "feature1__stddev_pop_over_2_minutes_fixed_windows": 0.0,
            "feature1__stddev_pop_over_15_minutes_fixed_windows": 0.0,
            "feature1__count_over_2_minutes_fixed_windows": 1,
            "feature1__count_over_15_minutes_fixed_windows": 1,
        },
        {
            "id": 2,
            TIMESTAMP_COLUMN: "2016-04-11 11:44:12",
            "feature1": 200,
            "feature2": 200,
            "feature1__avg_over_2_minutes_fixed_windows": 200.0,
            "feature1__avg_over_15_minutes_fixed_windows": 200.0,
            "feature1__stddev_pop_over_2_minutes_fixed_windows": 0.0,
            "feature1__stddev_pop_over_15_minutes_fixed_windows": 0.0,
            "feature1__count_over_2_minutes_fixed_windows": 1,
            "feature1__count_over_15_minutes_fixed_windows": 1,
        },
        {
            "id": 3,
            TIMESTAMP_COLUMN: "2016-04-11 11:46:24",
            "feature1": 300,
            "feature2": 300,
            "feature1__avg_over_2_minutes_fixed_windows": 300.0,
            "feature1__avg_over_15_minutes_fixed_windows": 300.0,
            "feature1__stddev_pop_over_2_minutes_fixed_windows": 0.0,
            "feature1__stddev_pop_over_15_minutes_fixed_windows": 0.0,
            "feature1__count_over_2_minutes_fixed_windows": 1,
            "feature1__count_over_15_minutes_fixed_windows": 1,
        },
        {
            "id": 4,
            TIMESTAMP_COLUMN: "2016-04-11 12:03:21",
            "feature1": 400,
            "feature2": 400,
            "feature1__avg_over_2_minutes_fixed_windows": 400.0,
            "feature1__avg_over_15_minutes_fixed_windows": 400.0,
            "feature1__stddev_pop_over_2_minutes_fixed_windows": 0.0,
            "feature1__stddev_pop_over_15_minutes_fixed_windows": 0.0,
            "feature1__count_over_2_minutes_fixed_windows": 1,
            "feature1__count_over_15_minutes_fixed_windows": 1,
        },
    ]
    df = spark_session.read.json(
        spark_context.parallelize(data).map(lambda x: json.dumps(x))
    )
    df = df.withColumn(TIMESTAMP_COLUMN, df.timestamp.cast(DataType.TIMESTAMP.value))
    return df


@fixture
def rolling_windows_target_df(spark_context, spark_session):
    data = [
        {
            "id": 1,
            TIMESTAMP_COLUMN: "2016-04-12 00:00:00",
            "feature1__avg_over_1_day_rolling_windows": 100.0,
            "feature1__avg_over_3_days_rolling_windows": 100.0,
            "feature1__stddev_pop_over_1_day_rolling_windows": 0.0,
            "feature1__stddev_pop_over_3_days_rolling_windows": 0.0,
            "feature1__count_over_1_day_rolling_windows": 1,
            "feature1__count_over_3_days_rolling_windows": 1,
        },
        {
            "id": 1,
            TIMESTAMP_COLUMN: "2016-04-13 00:00:00",
            "feature1__avg_over_1_day_rolling_windows": None,
            "feature1__avg_over_3_days_rolling_windows": 100.0,
            "feature1__stddev_pop_over_1_day_rolling_windows": None,
            "feature1__stddev_pop_over_3_days_rolling_windows": 0.0,
            "feature1__count_over_1_day_rolling_windows": None,
            "feature1__count_over_3_days_rolling_windows": 1,
        },
        {
            "id": 1,
            TIMESTAMP_COLUMN: "2016-04-14 00:00:00",
            "feature1__avg_over_1_day_rolling_windows": None,
            "feature1__avg_over_3_days_rolling_windows": 100.0,
            "feature1__stddev_pop_over_1_day_rolling_windows": None,
            "feature1__stddev_pop_over_3_days_rolling_windows": 0.0,
            "feature1__count_over_1_day_rolling_windows": None,
            "feature1__count_over_3_days_rolling_windows": 1,
        },
        {
            "id": 2,
            TIMESTAMP_COLUMN: "2016-04-12 00:00:00",
            "feature1__avg_over_1_day_rolling_windows": 200.0,
            "feature1__avg_over_3_days_rolling_windows": 200.0,
            "feature1__stddev_pop_over_1_day_rolling_windows": 0.0,
            "feature1__stddev_pop_over_3_days_rolling_windows": 0.0,
            "feature1__count_over_1_day_rolling_windows": 1,
            "feature1__count_over_3_days_rolling_windows": 1,
        },
        {
            "id": 2,
            TIMESTAMP_COLUMN: "2016-04-13 00:00:00",
            "feature1__avg_over_1_day_rolling_windows": None,
            "feature1__avg_over_3_days_rolling_windows": 200.0,
            "feature1__stddev_pop_over_1_day_rolling_windows": None,
            "feature1__stddev_pop_over_3_days_rolling_windows": 0.0,
            "feature1__count_over_1_day_rolling_windows": None,
            "feature1__count_over_3_days_rolling_windows": 1,
        },
        {
            "id": 2,
            TIMESTAMP_COLUMN: "2016-04-14 00:00:00",
            "feature1__avg_over_1_day_rolling_windows": None,
            "feature1__avg_over_3_days_rolling_windows": 200.0,
            "feature1__stddev_pop_over_1_day_rolling_windows": None,
            "feature1__stddev_pop_over_3_days_rolling_windows": 0.0,
            "feature1__count_over_1_day_rolling_windows": None,
            "feature1__count_over_3_days_rolling_windows": 1,
        },
        {
            "id": 3,
            TIMESTAMP_COLUMN: "2016-04-12 00:00:00",
            "feature1__avg_over_1_day_rolling_windows": 300.0,
            "feature1__avg_over_3_days_rolling_windows": 300.0,
            "feature1__stddev_pop_over_1_day_rolling_windows": 0.0,
            "feature1__stddev_pop_over_3_days_rolling_windows": 0.0,
            "feature1__count_over_1_day_rolling_windows": 1,
            "feature1__count_over_3_days_rolling_windows": 1,
        },
        {
            "id": 3,
            TIMESTAMP_COLUMN: "2016-04-13 00:00:00",
            "feature1__avg_over_1_day_rolling_windows": None,
            "feature1__avg_over_3_days_rolling_windows": 300.0,
            "feature1__stddev_pop_over_1_day_rolling_windows": None,
            "feature1__stddev_pop_over_3_days_rolling_windows": 0.0,
            "feature1__count_over_1_day_rolling_windows": None,
            "feature1__count_over_3_days_rolling_windows": 1,
        },
        {
            "id": 3,
            TIMESTAMP_COLUMN: "2016-04-14 00:00:00",
            "feature1__avg_over_1_day_rolling_windows": None,
            "feature1__avg_over_3_days_rolling_windows": 300.0,
            "feature1__stddev_pop_over_1_day_rolling_windows": None,
            "feature1__stddev_pop_over_3_days_rolling_windows": 0.0,
            "feature1__count_over_1_day_rolling_windows": None,
            "feature1__count_over_3_days_rolling_windows": 1,
        },
        {
            "id": 4,
            TIMESTAMP_COLUMN: "2016-04-12 00:00:00",
            "feature1__avg_over_1_day_rolling_windows": 400.0,
            "feature1__avg_over_3_days_rolling_windows": 400.0,
            "feature1__stddev_pop_over_1_day_rolling_windows": 0.0,
            "feature1__stddev_pop_over_3_days_rolling_windows": 0.0,
            "feature1__count_over_1_day_rolling_windows": 1,
            "feature1__count_over_3_days_rolling_windows": 1,
        },
        {
            "id": 4,
            TIMESTAMP_COLUMN: "2016-04-13 00:00:00",
            "feature1__avg_over_1_day_rolling_windows": None,
            "feature1__avg_over_3_days_rolling_windows": 400.0,
            "feature1__stddev_pop_over_1_day_rolling_windows": None,
            "feature1__stddev_pop_over_3_days_rolling_windows": 0.0,
            "feature1__count_over_1_day_rolling_windows": None,
            "feature1__count_over_3_days_rolling_windows": 1,
        },
        {
            "id": 4,
            TIMESTAMP_COLUMN: "2016-04-14 00:00:00",
            "feature1__avg_over_1_day_rolling_windows": None,
            "feature1__avg_over_3_days_rolling_windows": 400.0,
            "feature1__stddev_pop_over_1_day_rolling_windows": None,
            "feature1__stddev_pop_over_3_days_rolling_windows": 0.0,
            "feature1__count_over_1_day_rolling_windows": None,
            "feature1__count_over_3_days_rolling_windows": 1,
        },
    ]
    df = spark_session.read.json(
        spark_context.parallelize(data).map(lambda x: json.dumps(x))
    )
    df = df.withColumn(TIMESTAMP_COLUMN, df.timestamp.cast(DataType.TIMESTAMP.value))
    return df


@fixture
def sql_target_df(spark_context, spark_session):
    data = [
        {
            "id": 1,
            TIMESTAMP_COLUMN: 1460374271,
            "feature1": 100,
            "feature2": 100,
            "feature1_over_feature2": 1.0,
        },
        {
            "id": 2,
            TIMESTAMP_COLUMN: 1460375052,
            "feature1": 200,
            "feature2": 200,
            "feature1_over_feature2": 1.0,
        },
        {
            "id": 3,
            TIMESTAMP_COLUMN: 1460375184,
            "feature1": 300,
            "feature2": 300,
            "feature1_over_feature2": 1.0,
        },
        {
            "id": 4,
            TIMESTAMP_COLUMN: 1460376201,
            "feature1": 400,
            "feature2": 400,
            "feature1_over_feature2": 1.0,
        },
    ]
    df = spark_session.read.json(
        spark_context.parallelize(data).map(lambda x: json.dumps(x))
    )
    df = df.withColumn(TIMESTAMP_COLUMN, df.timestamp.cast(DataType.TIMESTAMP.value))
    return df
