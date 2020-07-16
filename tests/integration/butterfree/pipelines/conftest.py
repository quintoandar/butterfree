import pytest

from butterfree.constants import DataType
from butterfree.constants.columns import TIMESTAMP_COLUMN


@pytest.fixture()
def mocked_df(spark_context, spark_session):
    data = [
        {"id": 1, "origin_ts": "2016-04-11 11:31:11", "feature1": 200, "feature2": 200},
        {"id": 1, "origin_ts": "2016-04-11 11:44:12", "feature1": 300, "feature2": 300},
        {"id": 1, "origin_ts": "2016-04-11 11:46:24", "feature1": 400, "feature2": 400},
        {"id": 1, "origin_ts": "2016-04-11 12:03:21", "feature1": 500, "feature2": 500},
    ]
    df = spark_session.read.json(spark_context.parallelize(data, 1))
    df = df.withColumn(TIMESTAMP_COLUMN, df.origin_ts.cast(DataType.TIMESTAMP.spark))

    return df


@pytest.fixture()
def fixed_windows_output_feature_set_dataframe(spark_context, spark_session):
    data = [
        {
            "id": 1,
            "timestamp": "2016-04-11 11:31:11",
            "feature1__avg_over_2_minutes_fixed_windows": 200,
            "feature1__avg_over_15_minutes_fixed_windows": 200,
            "feature1__stddev_pop_over_2_minutes_fixed_windows": 0,
            "feature1__stddev_pop_over_15_minutes_fixed_windows": 0,
            "divided_feature": 1,
            "year": 2016,
            "month": 4,
            "day": 11,
        },
        {
            "id": 1,
            "timestamp": "2016-04-11 11:44:12",
            "feature1__avg_over_2_minutes_fixed_windows": 300,
            "feature1__avg_over_15_minutes_fixed_windows": 250,
            "feature1__stddev_pop_over_2_minutes_fixed_windows": 0,
            "feature1__stddev_pop_over_15_minutes_fixed_windows": 50,
            "divided_feature": 1,
            "year": 2016,
            "month": 4,
            "day": 11,
        },
        {
            "id": 1,
            "timestamp": "2016-04-11 11:46:24",
            "feature1__avg_over_2_minutes_fixed_windows": 400,
            "feature1__avg_over_15_minutes_fixed_windows": 350,
            "feature1__stddev_pop_over_2_minutes_fixed_windows": 0,
            "feature1__stddev_pop_over_15_minutes_fixed_windows": 50,
            "divided_feature": 1,
            "year": 2016,
            "month": 4,
            "day": 11,
        },
        {
            "id": 1,
            "timestamp": "2016-04-11 12:03:21",
            "feature1__avg_over_2_minutes_fixed_windows": 500,
            "feature1__avg_over_15_minutes_fixed_windows": 500,
            "feature1__stddev_pop_over_2_minutes_fixed_windows": 0,
            "feature1__stddev_pop_over_15_minutes_fixed_windows": 0,
            "divided_feature": 1,
            "year": 2016,
            "month": 4,
            "day": 11,
        },
    ]
    df = spark_session.read.json(spark_context.parallelize(data, 1))
    df = df.withColumn(TIMESTAMP_COLUMN, df.timestamp.cast(DataType.TIMESTAMP.spark))

    return df
