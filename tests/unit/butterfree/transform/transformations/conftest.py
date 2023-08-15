import json

from pytest import fixture

from butterfree.constants.columns import TIMESTAMP_COLUMN
from butterfree.constants.data_type import DataType


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
def feature_set_df_pivot(spark_context, spark_session):
    data = [
        {
            "id": 1,
            "timestamp": "2016-04-11 11:31:11",
            "feature1": 200,
            "pivot_col": "N",
        },
        {
            "id": 1,
            "timestamp": "2016-04-11 11:44:12",
            "feature1": 300,
            "pivot_col": "N",
        },
        {
            "id": 1,
            "timestamp": "2016-04-11 11:46:24",
            "feature1": 400,
            "pivot_col": "S",
        },
        {
            "id": 1,
            "timestamp": "2016-04-11 12:03:21",
            "feature1": 500,
            "pivot_col": "S",
        },
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
    df = df.withColumn(TIMESTAMP_COLUMN, df.timestamp.cast(DataType.TIMESTAMP.spark))

    return df


@fixture
def target_df_agg(spark_context, spark_session):
    data = [
        {"id": 1, "feature1__avg": 350, "feature1__stddev_pop": 111.80339887498948},
    ]
    df = spark_session.read.json(spark_context.parallelize(data, 1))

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
    df = df.withColumn(TIMESTAMP_COLUMN, df.timestamp.cast(DataType.TIMESTAMP.spark))

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
    df = df.withColumn(TIMESTAMP_COLUMN, df.timestamp.cast(DataType.TIMESTAMP.spark))

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
def target_df_pivot_agg(spark_context, spark_session):
    data = [
        {
            "id": 1,
            "S_feature__avg": 450,
            "S_feature__stddev_pop": 50,
            "N_feature__avg": 250,
            "N_feature__stddev_pop": 50,
        },
    ]
    df = spark_session.read.json(spark_context.parallelize(data, 1))

    return df


@fixture
def target_df_pivot_agg_window(spark_context, spark_session):
    data = [
        {
            "id": 1,
            "timestamp": "2016-04-12 00:00:00",
            "S_feature__avg_over_2_days_rolling_windows": 450,
            "S_feature__stddev_pop_over_2_days_rolling_windows": 50,
            "N_feature__avg_over_2_days_rolling_windows": 250,
            "N_feature__stddev_pop_over_2_days_rolling_windows": 50,
        },
        {
            "id": 1,
            "timestamp": "2016-04-13 00:00:00",
            "S_feature__avg_over_2_days_rolling_windows": 450,
            "S_feature__stddev_pop_over_2_days_rolling_windows": 50,
            "N_feature__avg_over_2_days_rolling_windows": 250,
            "N_feature__stddev_pop_over_2_days_rolling_windows": 50,
        },
    ]
    df = spark_session.read.json(
        spark_context.parallelize(data).map(lambda x: json.dumps(x))
    )
    df = df.withColumn(TIMESTAMP_COLUMN, df.timestamp.cast(DataType.TIMESTAMP.spark))

    return df


@fixture
def target_df_rolling_agg(spark_context, spark_session):
    data = [
        {
            "id": 1,
            "timestamp": "2016-04-12",
            "feature1__avg_over_1_day_rolling_windows": 350,
            "feature1__avg_over_1_week_rolling_windows": 350,
            "feature1__stddev_pop_over_1_day_rolling_windows": 111.80339887498948,
            "feature1__stddev_pop_over_1_week_rolling_windows": 111.80339887498948,
            "feature1__count_over_1_day_rolling_windows": 4,
            "feature1__count_over_1_week_rolling_windows": 4,
        },
        {
            "id": 1,
            "timestamp": "2016-04-13",
            "feature1__avg_over_1_day_rolling_windows": None,
            "feature1__avg_over_1_week_rolling_windows": 350,
            "feature1__stddev_pop_over_1_day_rolling_windows": None,
            "feature1__stddev_pop_over_1_week_rolling_windows": 111.80339887498948,
            "feature1__count_over_1_day_rolling_windows": None,
            "feature1__count_over_1_week_rolling_windows": 4,
        },
        {
            "id": 1,
            "timestamp": "2016-04-14",
            "feature1__avg_over_1_day_rolling_windows": None,
            "feature1__avg_over_1_week_rolling_windows": 350,
            "feature1__stddev_pop_over_1_day_rolling_windows": None,
            "feature1__stddev_pop_over_1_week_rolling_windows": 111.80339887498948,
            "feature1__count_over_1_day_rolling_windows": None,
            "feature1__count_over_1_week_rolling_windows": 4,
        },
        {
            "id": 1,
            "timestamp": "2016-04-15",
            "feature1__avg_over_1_day_rolling_windows": None,
            "feature1__avg_over_1_week_rolling_windows": 350,
            "feature1__stddev_pop_over_1_day_rolling_windows": None,
            "feature1__stddev_pop_over_1_week_rolling_windows": 111.80339887498948,
            "feature1__count_over_1_day_rolling_windows": None,
            "feature1__count_over_1_week_rolling_windows": 4,
        },
        {
            "id": 1,
            "timestamp": "2016-04-16",
            "feature1__avg_over_1_day_rolling_windows": None,
            "feature1__avg_over_1_week_rolling_windows": 350,
            "feature1__stddev_pop_over_1_day_rolling_windows": None,
            "feature1__stddev_pop_over_1_week_rolling_windows": 111.80339887498948,
            "feature1__count_over_1_day_rolling_windows": None,
            "feature1__count_over_1_week_rolling_windows": 4,
        },
        {
            "id": 1,
            "timestamp": "2016-04-17",
            "feature1__avg_over_1_day_rolling_windows": None,
            "feature1__avg_over_1_week_rolling_windows": 350,
            "feature1__stddev_pop_over_1_day_rolling_windows": None,
            "feature1__stddev_pop_over_1_week_rolling_windows": 111.80339887498948,
            "feature1__count_over_1_day_rolling_windows": None,
            "feature1__count_over_1_week_rolling_windows": 4,
        },
        {
            "id": 1,
            "timestamp": "2016-04-18",
            "feature1__avg_over_1_day_rolling_windows": None,
            "feature1__avg_over_1_week_rolling_windows": 350,
            "feature1__stddev_pop_over_1_day_rolling_windows": None,
            "feature1__stddev_pop_over_1_week_rolling_windows": 111.80339887498948,
            "feature1__count_over_1_day_rolling_windows": None,
            "feature1__count_over_1_week_rolling_windows": 4,
        },
    ]
    df = spark_session.read.json(
        spark_context.parallelize(data).map(lambda x: json.dumps(x))
    )
    df = df.withColumn(TIMESTAMP_COLUMN, df.timestamp.cast(DataType.TIMESTAMP.spark))

    return df


@fixture
def h3_input_df(spark_context, spark_session):
    data = [
        {"lat": -23.554190, "lng": -46.670723},
    ]
    return spark_session.read.json(spark_context.parallelize(data, 1))


@fixture
def h3_target_data():
    return [
        {
            "lat": -23.554190,
            "lng": -46.670723,
            "lat_lng__h3_hash__6": "86a8100efffffff",
            "lat_lng__h3_hash__7": "87a8100eaffffff",
            "lat_lng__h3_hash__8": "88a8100ea1fffff",
            "lat_lng__h3_hash__9": "89a8100ea0fffff",
            "lat_lng__h3_hash__10": "8aa8100ea0d7fff",
            "lat_lng__h3_hash__11": "8ba8100ea0d5fff",
            "lat_lng__h3_hash__12": "8ca8100ea0d57ff",
        },
    ]


@fixture
def h3_target_df(h3_target_data, spark_context, spark_session):
    return spark_session.read.json(spark_context.parallelize(h3_target_data, 1))


@fixture()
def h3_with_stack_target_df(h3_target_data, spark_context, spark_session):
    resolutions = [f"lat_lng__h3_hash__{r}" for r in range(6, 13)]
    h3_target_data_dict = h3_target_data.pop()
    data_with_stack = [
        {"id": h3_target_data_dict[resolution], **h3_target_data_dict}
        for resolution in resolutions
    ]
    return spark_session.read.json(spark_context.parallelize(data_with_stack, 1))


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
