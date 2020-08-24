import pytest
from pyspark.sql import functions as F

from butterfree.constants import DataType
from butterfree.constants.columns import TIMESTAMP_COLUMN
from butterfree.dataframe_service.incremental_strategy import IncrementalStrategy
from butterfree.extract import Source
from butterfree.extract.readers import TableReader
from butterfree.load import Sink
from butterfree.load.writers import HistoricalFeatureStoreWriter
from butterfree.pipelines.feature_set_pipeline import FeatureSetPipeline
from butterfree.transform import FeatureSet
from butterfree.transform.features import Feature, KeyFeature, TimestampFeature
from butterfree.transform.transformations import SparkFunctionTransform
from butterfree.transform.utils import Function


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


@pytest.fixture()
def mocked_date_df(spark_context, spark_session):
    data = [
        {"id": 1, "ts": "2016-04-11 11:31:11", "feature": 200},
        {"id": 1, "ts": "2016-04-12 11:44:12", "feature": 300},
        {"id": 1, "ts": "2016-04-13 11:46:24", "feature": 400},
        {"id": 1, "ts": "2016-04-14 12:03:21", "feature": 500},
    ]
    df = spark_session.read.json(spark_context.parallelize(data, 1))
    df = df.withColumn(TIMESTAMP_COLUMN, df.ts.cast(DataType.TIMESTAMP.spark))

    return df


@pytest.fixture()
def fixed_windows_output_feature_set_date_dataframe(spark_context, spark_session):
    data = [
        {
            "id": 1,
            "timestamp": "2016-04-12 11:44:12",
            "feature__avg_over_1_day_fixed_windows": 300,
            "feature__stddev_pop_over_1_day_fixed_windows": 0,
            "year": 2016,
            "month": 4,
            "day": 12,
        },
        {
            "id": 1,
            "timestamp": "2016-04-13 11:46:24",
            "feature__avg_over_1_day_fixed_windows": 400,
            "feature__stddev_pop_over_1_day_fixed_windows": 0,
            "year": 2016,
            "month": 4,
            "day": 13,
        },
    ]
    df = spark_session.read.json(spark_context.parallelize(data, 1))
    df = df.withColumn(TIMESTAMP_COLUMN, df.timestamp.cast(DataType.TIMESTAMP.spark))

    return df


@pytest.fixture()
def feature_set_pipeline(spark_context, spark_session):
    feature_set_pipeline = FeatureSetPipeline(
        source=Source(
            readers=[
                TableReader(id="b_source", table="b_table",).with_incremental_strategy(
                    incremental_strategy=IncrementalStrategy(column="timestamp")
                ),
            ],
            query=f"select * from b_source ",  # noqa
        ),
        feature_set=FeatureSet(
            name="feature_set",
            entity="entity",
            description="description",
            features=[
                Feature(
                    name="feature",
                    description="test",
                    transformation=SparkFunctionTransform(
                        functions=[
                            Function(F.avg, DataType.FLOAT),
                            Function(F.stddev_pop, DataType.FLOAT),
                        ],
                    ).with_window(
                        partition_by="id",
                        order_by=TIMESTAMP_COLUMN,
                        mode="fixed_windows",
                        window_definition=["1 day"],
                    ),
                ),
            ],
            keys=[
                KeyFeature(
                    name="id",
                    description="The user's Main ID or device ID",
                    dtype=DataType.INTEGER,
                )
            ],
            timestamp=TimestampFeature(),
        ),
        sink=Sink(writers=[HistoricalFeatureStoreWriter(debug_mode=True)]),
    )

    return feature_set_pipeline
