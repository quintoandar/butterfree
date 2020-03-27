import json

from pytest import fixture

from butterfree.core.constants.columns import TIMESTAMP_COLUMN
from butterfree.core.constants.data_type import DataType


@fixture
def feature_set_dataframe(spark_context, spark_session):
    data = [
        {"id": 1, "timestamp": "2016-04-11 11:31:11", "feature1": 200, "feature2": 200},
        {"id": 1, "timestamp": "2016-04-11 11:44:12", "feature1": 300, "feature2": 300},
        {"id": 1, "timestamp": "2016-04-11 11:46:24", "feature1": 400, "feature2": 400},
        {"id": 1, "timestamp": "2016-04-11 12:03:21", "feature1": 500, "feature2": 500},
    ]
    df = spark_session.read.json(spark_context.parallelize(data, 1))
    df = df.withColumn(TIMESTAMP_COLUMN, df.timestamp.cast(DataType.TIMESTAMP.spark))

    return df


@fixture
def target_df_spark(spark_context, spark_session):
    data = [
        {
            "id": 1,
            "timestamp": "2016-04-11 11:31:11",
            "feature1": 200,
            "feature2": 200,
            "feature__cos": 0.4871876750070059,
        },
        {
            "id": 1,
            "timestamp": "2016-04-11 11:44:12",
            "feature1": 300,
            "feature2": 300,
            "feature__cos": -0.022096619278683942,
        },
        {
            "id": 1,
            "timestamp": "2016-04-11 11:46:24",
            "feature1": 400,
            "feature2": 400,
            "feature__cos": -0.525296338642536,
        },
        {
            "id": 1,
            "timestamp": "2016-04-11 12:03:21",
            "feature1": 500,
            "feature2": 500,
            "feature__cos": -0.883849273431478,
        },
    ]
    df = spark_session.read.json(spark_context.parallelize(data, 1))
    df = df.withColumn(TIMESTAMP_COLUMN, df.timestamp.cast(DataType.TIMESTAMP.value))

    return df


@fixture
def most_common_dataframe(spark_context, spark_session):
    data = [
        {"id": 1, "timestamp": "2016-04-11 11:31:11", "feature1": 200, "feature2": 200},
        {"id": 1, "timestamp": "2016-04-11 11:44:12", "feature1": 200, "feature2": 300},
        {"id": 1, "timestamp": "2016-04-11 11:46:24", "feature1": 200, "feature2": 400},
        {"id": 1, "timestamp": "2016-04-11 12:03:21", "feature1": 300, "feature2": 500},
        {"id": 1, "timestamp": "2016-04-11 12:06:21", "feature1": 300, "feature2": 500},
        {"id": 1, "timestamp": "2016-04-11 12:09:21", "feature1": 300, "feature2": 500},
        {"id": 1, "timestamp": "2016-04-11 12:23:21", "feature1": 300, "feature2": 500},
        {"id": 1, "timestamp": "2016-04-11 12:45:21", "feature1": 300, "feature2": 500},
        {"id": 1, "timestamp": "2016-04-11 12:56:21", "feature1": 300, "feature2": 500},
    ]
    df = spark_session.read.json(spark_context.parallelize(data, 1))
    df = df.withColumn(TIMESTAMP_COLUMN, df.timestamp.cast(DataType.TIMESTAMP.spark))

    return df


@fixture
def most_common_output_dataframe(spark_context, spark_session):
    data = [
        {
            "id": 1,
            "timestamp": "2016-04-11 11:31:11",
            "feature1": 200,
            "feature2": 200,
            "feature1__avg_over_4_events_row_windows": 200,
            "feature1__most_common_over_4_events_row_windows": [200],
        },
        {
            "id": 1,
            "timestamp": "2016-04-11 11:44:12",
            "feature1": 200,
            "feature2": 300,
            "feature1__avg_over_4_events_row_windows": 200,
            "feature1__most_common_over_4_events_row_windows": [200],
        },
        {
            "id": 1,
            "timestamp": "2016-04-11 11:46:24",
            "feature1": 200,
            "feature2": 400,
            "feature1__avg_over_4_events_row_windows": 200,
            "feature1__most_common_over_4_events_row_windows": [200],
        },
        {
            "id": 1,
            "timestamp": "2016-04-11 12:03:21",
            "feature1": 300,
            "feature2": 500,
            "feature1__avg_over_4_events_row_windows": 225,
            "feature1__most_common_over_4_events_row_windows": [200],
        },
        {
            "id": 1,
            "timestamp": "2016-04-11 12:06:21",
            "feature1": 300,
            "feature2": 500,
            "feature1__avg_over_4_events_row_windows": 250,
            "feature1__most_common_over_4_events_row_windows": [200],
        },
        {
            "id": 1,
            "timestamp": "2016-04-11 12:09:21",
            "feature1": 300,
            "feature2": 500,
            "feature1__avg_over_4_events_row_windows": 275,
            "feature1__most_common_over_4_events_row_windows": [300],
        },
        {
            "id": 1,
            "timestamp": "2016-04-11 12:23:21",
            "feature1": 300,
            "feature2": 500,
            "feature1__avg_over_4_events_row_windows": 300,
            "feature1__most_common_over_4_events_row_windows": [300],
        },
        {
            "id": 1,
            "timestamp": "2016-04-11 12:45:21",
            "feature1": 300,
            "feature2": 500,
            "feature1__avg_over_4_events_row_windows": 300,
            "feature1__most_common_over_4_events_row_windows": [300],
        },
        {
            "id": 1,
            "timestamp": "2016-04-11 12:56:21",
            "feature1": 300,
            "feature2": 500,
            "feature1__avg_over_4_events_row_windows": 300,
            "feature1__most_common_over_4_events_row_windows": [300],
        },
    ]
    df = spark_session.read.json(spark_context.parallelize(data, 1))
    df = df.withColumn(TIMESTAMP_COLUMN, df.timestamp.cast(DataType.TIMESTAMP.spark))

    return df


@fixture
def target_df_rows_agg(spark_context, spark_session):
    data = [
        {
            "id": 1,
            "timestamp": "2016-04-11 11:31:11",
            "feature1": 200,
            "feature2": 200,
            "feature1__avg_over_2_events_row_windows": 200,
            "feature1__avg_over_3_events_row_windows": 200,
        },
        {
            "id": 1,
            "timestamp": "2016-04-11 11:44:12",
            "feature1": 300,
            "feature2": 300,
            "feature1__avg_over_2_events_row_windows": 250,
            "feature1__avg_over_3_events_row_windows": 250,
        },
        {
            "id": 1,
            "timestamp": "2016-04-11 11:46:24",
            "feature1": 400,
            "feature2": 400,
            "feature1__avg_over_2_events_row_windows": 350,
            "feature1__avg_over_3_events_row_windows": 300,
        },
        {
            "id": 1,
            "timestamp": "2016-04-11 12:03:21",
            "feature1": 500,
            "feature2": 500,
            "feature1__avg_over_2_events_row_windows": 450,
            "feature1__avg_over_3_events_row_windows": 400,
        },
    ]
    df = spark_session.read.json(spark_context.parallelize(data, 1))
    df = df.withColumn(TIMESTAMP_COLUMN, df.timestamp.cast(DataType.TIMESTAMP.value))

    return df


@fixture
def target_df_rows_agg_2(spark_context, spark_session):
    data = [
        {
            "id": 1,
            "timestamp": "2016-04-11 11:31:11",
            "feature1": 200,
            "feature2": 200,
            "feature1__avg_over_2_events_row_windows": 200,
        },
        {
            "id": 1,
            "timestamp": "2016-04-11 11:44:12",
            "feature1": 300,
            "feature2": 300,
            "feature1__avg_over_2_events_row_windows": 250,
        },
        {
            "id": 1,
            "timestamp": "2016-04-11 11:46:24",
            "feature1": 400,
            "feature2": 400,
            "feature1__avg_over_2_events_row_windows": 350,
        },
        {
            "id": 1,
            "timestamp": "2016-04-11 12:03:21",
            "feature1": 500,
            "feature2": 500,
            "feature1__avg_over_2_events_row_windows": 450,
        },
    ]
    df = spark_session.read.json(spark_context.parallelize(data, 1))
    df = df.withColumn(TIMESTAMP_COLUMN, df.timestamp.cast(DataType.TIMESTAMP.value))

    return df


@fixture
def target_df_fixed_agg(spark_context, spark_session):
    data = [
        {
            "id": 1,
            "timestamp": "2016-04-11 11:31:11",
            "feature1": 200,
            "feature2": 200,
            "feature1__avg_over_2_minutes_fixed_windows": 200,
            "feature1__avg_over_15_minutes_fixed_windows": 200,
        },
        {
            "id": 1,
            "timestamp": "2016-04-11 11:44:12",
            "feature1": 300,
            "feature2": 300,
            "feature1__avg_over_2_minutes_fixed_windows": 300,
            "feature1__avg_over_15_minutes_fixed_windows": 250,
        },
        {
            "id": 1,
            "timestamp": "2016-04-11 11:46:24",
            "feature1": 400,
            "feature2": 400,
            "feature1__avg_over_2_minutes_fixed_windows": 400,
            "feature1__avg_over_15_minutes_fixed_windows": 350,
        },
        {
            "id": 1,
            "timestamp": "2016-04-11 12:03:21",
            "feature1": 500,
            "feature2": 500,
            "feature1__avg_over_2_minutes_fixed_windows": 500,
            "feature1__avg_over_15_minutes_fixed_windows": 500,
        },
    ]
    df = spark_session.read.json(spark_context.parallelize(data, 1))
    df = df.withColumn(TIMESTAMP_COLUMN, df.timestamp.cast(DataType.TIMESTAMP.spark))

    return df


@fixture
def h3_dataframe(spark_context, spark_session):
    data = [
        {"id": 1, "feature": 200, "lat": -23.554190, "lng": -46.670723},
        {"id": 1, "feature": 300, "lat": -23.554190, "lng": -46.670723},
        {"id": 1, "feature": 400, "lat": -23.554190, "lng": -46.670723},
        {"id": 1, "feature": 500, "lat": -23.554190, "lng": -46.670723},
    ]
    df = spark_session.read.json(spark_context.parallelize(data, 1))

    return df


@fixture
def with_house_ids_dataframe(spark_context, spark_session):
    data = [
        {
            "user_id": 1,
            "house_id": 123,
            "ts": "2016-04-11 00:00:00",
            "feature1": 200,
            "feature2": 200,
            "nonfeature": 0,
        },
        {
            "user_id": 1,
            "house_id": 400,
            "ts": "2016-04-11 00:00:05",
            "feature1": 300,
            "feature2": 300,
            "nonfeature": 0,
        },
        {
            "user_id": 1,
            "house_id": 192,
            "ts": "2016-04-12 00:00:00",
            "feature1": 400,
            "feature2": 400,
            "nonfeature": 0,
        },
        {
            "user_id": 1,
            "house_id": 715,
            "ts": "2016-04-15 00:00:00",
            "feature1": 500,
            "feature2": 500,
            "nonfeature": 0,
        },
    ]
    df = spark_session.read.json(
        spark_context.parallelize(data).map(lambda x: json.dumps(x))
    )
    df = df.withColumn(TIMESTAMP_COLUMN, df.ts.cast(DataType.TIMESTAMP.spark))

    return df


@fixture
def mode_dataframe(spark_context, spark_session):
    data = [
        {"id": 1, "timestamp": "2016-04-11 11:31:11", "feature1": 200},
        {"id": 1, "timestamp": "2016-04-11 11:44:12", "feature1": 200},
        {"id": 1, "timestamp": "2016-04-11 11:46:24", "feature1": 200},
        {"id": 1, "timestamp": "2016-04-11 12:03:21", "feature1": 300},
        {"id": 1, "timestamp": "2016-04-12 11:31:11", "feature1": 300},
        {"id": 1, "timestamp": "2016-04-12 11:44:12", "feature1": 300},
        {"id": 1, "timestamp": "2016-04-12 11:46:24", "feature1": 300},
        {"id": 1, "timestamp": "2016-04-12 12:03:21", "feature1": 300},
    ]
    df = spark_session.read.json(spark_context.parallelize(data, 1))
    df = df.withColumn(TIMESTAMP_COLUMN, df.timestamp.cast(DataType.TIMESTAMP.spark))

    return df


@fixture
def mode_str_target_dataframe(spark_context, spark_session):
    data = [
        {
            "id": 1,
            "timestamp": "2016-04-12 00:00:00",
            "feature1__mode_over_1_day_rolling_windows": "200",
        },
        {
            "id": 1,
            "timestamp": "2016-04-13 00:00:00",
            "feature1__mode_over_1_day_rolling_windows": "300",
        },
    ]
    df = spark_session.read.json(spark_context.parallelize(data, 1))
    df = df.withColumn(TIMESTAMP_COLUMN, df.timestamp.cast(DataType.TIMESTAMP.spark))

    return df


@fixture
def mode_num_target_dataframe(spark_context, spark_session):
    data = [
        {
            "id": 1,
            "timestamp": "2016-04-12 00:00:00",
            "feature1__mode_over_1_day_rolling_windows": 200,
        },
        {
            "id": 1,
            "timestamp": "2016-04-13 00:00:00",
            "feature1__mode_over_1_day_rolling_windows": 300,
        },
    ]
    df = spark_session.read.json(spark_context.parallelize(data, 1))
    df = df.withColumn(TIMESTAMP_COLUMN, df.timestamp.cast(DataType.TIMESTAMP.spark))

    return df
