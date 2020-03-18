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
    df = df.withColumn(TIMESTAMP_COLUMN, df.timestamp.cast(DataType.TIMESTAMP.value))

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
            "feature1__stddev_pop_over_2_events_row_windows": 0,
            "feature1__count_over_2_events_row_windows": 1,
        },
        {
            "id": 1,
            "timestamp": "2016-04-11 11:44:12",
            "feature1": 300,
            "feature2": 300,
            "feature1__avg_over_2_events_row_windows": 250,
            "feature1__stddev_pop_over_2_events_row_windows": 50,
            "feature1__count_over_2_events_row_windows": 2,
        },
        {
            "id": 1,
            "timestamp": "2016-04-11 11:46:24",
            "feature1": 400,
            "feature2": 400,
            "feature1__avg_over_2_events_row_windows": 350,
            "feature1__stddev_pop_over_2_events_row_windows": 50,
            "feature1__count_over_2_events_row_windows": 2,
        },
        {
            "id": 1,
            "timestamp": "2016-04-11 12:03:21",
            "feature1": 500,
            "feature2": 500,
            "feature1__avg_over_2_events_row_windows": 450,
            "feature1__stddev_pop_over_2_events_row_windows": 50,
            "feature1__count_over_2_events_row_windows": 2,
        },
    ]
    df = spark_session.read.json(spark_context.parallelize(data, 1))
    df = df.withColumn(TIMESTAMP_COLUMN, df.timestamp.cast(DataType.TIMESTAMP.value))

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
    df = df.withColumn(TIMESTAMP_COLUMN, df.ts.cast(DataType.TIMESTAMP.value))

    return df


@fixture()
def input_df(spark_context, spark_session):
    data = [
        {
            "id": 1,
            TIMESTAMP_COLUMN: "2016-04-11 11:31:11",
            "pivot_column": 1,
            "has_feature": True,
        },
        {
            "id": 1,
            TIMESTAMP_COLUMN: "2016-04-11 11:44:12",
            "pivot_column": 2,
            "has_feature": True,
        },
        {
            "id": 1,
            TIMESTAMP_COLUMN: "2016-04-11 11:46:24",
            "pivot_column": 3,
            "has_feature": True,
        },
        {
            "id": 1,
            TIMESTAMP_COLUMN: "2016-04-11 12:03:21",
            "pivot_column": 4,
            "has_feature": True,
        },
        {
            "id": 2,
            TIMESTAMP_COLUMN: "2016-04-11 12:03:21",
            "pivot_column": 4,
            "has_feature": True,
        },
    ]
    df = spark_session.read.json(
        spark_context.parallelize(data).map(lambda x: json.dumps(x))
    )
    return df


@fixture()
def pivot_df(spark_context, spark_session):
    data = [
        {
            "id": 1,
            TIMESTAMP_COLUMN: "2016-04-11 11:31:11",
            "1__count": 1,
            "2__count": None,
            "3__count": None,
            "4__count": None,
        },
        {
            "id": 1,
            TIMESTAMP_COLUMN: "2016-04-11 11:44:12",
            "1__count": None,
            "2__count": 1,
            "3__count": None,
            "4__count": None,
        },
        {
            "id": 1,
            TIMESTAMP_COLUMN: "2016-04-11 11:46:24",
            "1__count": None,
            "2__count": None,
            "3__count": 1,
            "4__count": None,
        },
        {
            "id": 1,
            TIMESTAMP_COLUMN: "2016-04-11 12:03:21",
            "1__count": None,
            "2__count": None,
            "3__count": None,
            "4__count": 1,
        },
        {
            "id": 2,
            TIMESTAMP_COLUMN: "2016-04-11 12:03:21",
            "1__count": None,
            "2__count": None,
            "3__count": None,
            "4__count": 1,
        },
    ]
    df = spark_session.read.json(
        spark_context.parallelize(data).map(lambda x: json.dumps(x))
    )
    return df.orderBy(TIMESTAMP_COLUMN)


@fixture()
def pivot_ffill_df(spark_context, spark_session):
    data = [
        {
            "id": 1,
            TIMESTAMP_COLUMN: "2016-04-11 11:31:11",
            "1__count": 1,
            "2__count": None,
            "3__count": None,
            "4__count": None,
        },
        {
            "id": 1,
            TIMESTAMP_COLUMN: "2016-04-11 11:44:12",
            "1__count": 1,
            "2__count": 1,
            "3__count": None,
            "4__count": None,
        },
        {
            "id": 1,
            TIMESTAMP_COLUMN: "2016-04-11 11:46:24",
            "1__count": 1,
            "2__count": 1,
            "3__count": 1,
            "4__count": None,
        },
        {
            "id": 1,
            TIMESTAMP_COLUMN: "2016-04-11 12:03:21",
            "1__count": 1,
            "2__count": 1,
            "3__count": 1,
            "4__count": 1,
        },
        {
            "id": 2,
            TIMESTAMP_COLUMN: "2016-04-11 12:03:21",
            "1__count": None,
            "2__count": None,
            "3__count": None,
            "4__count": 1,
        },
    ]
    df = spark_session.read.json(
        spark_context.parallelize(data).map(lambda x: json.dumps(x))
    )
    return df.orderBy(TIMESTAMP_COLUMN)
