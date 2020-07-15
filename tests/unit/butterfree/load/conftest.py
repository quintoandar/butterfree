from pyspark.sql import functions
from pyspark.sql.types import StringType, StructField, StructType
from pytest import fixture

from butterfree.configs.db import CassandraConfig
from butterfree.constants import columns
from butterfree.constants.columns import TIMESTAMP_COLUMN
from butterfree.constants.data_type import DataType
from butterfree.transform import FeatureSet
from butterfree.transform.aggregated_feature_set import AggregatedFeatureSet
from butterfree.transform.features import Feature, KeyFeature, TimestampFeature
from butterfree.transform.transformations import AggregatedTransform
from butterfree.transform.utils import Function


@fixture
def feature_set():
    key_features = [
        KeyFeature(name="id", description="Description", dtype=DataType.INTEGER)
    ]
    ts_feature = TimestampFeature(from_column=TIMESTAMP_COLUMN)
    features = [
        Feature(name="feature", description="Description", dtype=DataType.BIGINT,)
    ]
    return FeatureSet(
        "feature_set",
        "entity",
        "description",
        keys=key_features,
        timestamp=ts_feature,
        features=features,
    )


@fixture
def feature_set_dataframe(spark_context, spark_session):
    data = [
        {"id": 1, TIMESTAMP_COLUMN: "2019-12-31", "feature": 100},
        {"id": 2, TIMESTAMP_COLUMN: "2019-12-31", "feature": 200},
        {"id": 1, TIMESTAMP_COLUMN: "2020-01-15", "feature": 110},
        {"id": 1, TIMESTAMP_COLUMN: "2020-02-01", "feature": 120},
    ]
    return spark_session.read.json(spark_context.parallelize(data, 1))


@fixture
def historical_feature_set_dataframe(spark_context, spark_session):
    data = [
        {
            "feature": 100,
            "id": 1,
            TIMESTAMP_COLUMN: "2019-12-31",
            columns.PARTITION_YEAR: 2019,
            columns.PARTITION_MONTH: 12,
            columns.PARTITION_DAY: 31,
        },
        {
            "id": 2,
            TIMESTAMP_COLUMN: "2019-12-31",
            "feature": 200,
            columns.PARTITION_YEAR: 2019,
            columns.PARTITION_MONTH: 12,
            columns.PARTITION_DAY: 31,
        },
        {
            "id": 1,
            TIMESTAMP_COLUMN: "2020-01-15",
            "feature": 110,
            columns.PARTITION_YEAR: 2020,
            columns.PARTITION_MONTH: 1,
            columns.PARTITION_DAY: 15,
        },
        {
            "id": 1,
            TIMESTAMP_COLUMN: "2020-02-01",
            "feature": 120,
            columns.PARTITION_YEAR: 2020,
            columns.PARTITION_MONTH: 2,
            columns.PARTITION_DAY: 1,
        },
    ]
    return spark_session.read.json(spark_context.parallelize(data, 1))


@fixture
def latest_feature_set_dataframe(spark_context, spark_session):
    data = [
        {"id": 2, TIMESTAMP_COLUMN: "2019-12-31", "feature": 200},
        {"id": 1, TIMESTAMP_COLUMN: "2020-02-01", "feature": 120},
    ]
    return spark_session.read.json(spark_context.parallelize(data, 1))


@fixture
def feature_set_dataframe_without_ts(spark_context, spark_session):
    data = [
        {"id": 1, "feature": 100},
        {"id": 2, "feature": 200},
        {"id": 1, "feature": 110},
        {"id": 1, "feature": 120},
    ]
    return spark_session.read.json(spark_context.parallelize(data, 1))


@fixture
def count_feature_set_dataframe(spark_context, spark_session):
    data = [
        {"row": 4},
    ]
    return spark_session.read.json(spark_context.parallelize(data, 1))


@fixture
def not_feature_set_dataframe():
    data = "not a spark df writer"
    return data


@fixture
def empty_feature_set_dataframe(spark_context, spark_session):

    field = [StructField("field1", StringType(), True)]
    schema = StructType(field)

    return spark_session.createDataFrame(spark_context.emptyRDD(), schema)


@fixture
def cassandra_config():
    return CassandraConfig(keyspace="feature_set")


@fixture(params=["feature_set_empty", "feature_set_without_ts", "feature_set_not_df"])
def feature_sets(request):
    return request.getfixturevalue(request.param)


@fixture
def test_feature_set():
    return AggregatedFeatureSet(
        name="feature_set",
        entity="entity",
        description="description",
        features=[
            Feature(
                name="feature1",
                description="test",
                transformation=AggregatedTransform(
                    functions=[
                        Function(functions.avg, DataType.DOUBLE),
                        Function(functions.stddev_pop, DataType.DOUBLE),
                    ]
                ),
            ),
            Feature(
                name="feature2",
                description="test",
                transformation=AggregatedTransform(
                    functions=[Function(functions.count, DataType.INTEGER)]
                ),
            ),
        ],
        keys=[
            KeyFeature(
                name="id",
                description="The user's Main ID or device ID",
                dtype=DataType.BIGINT,
            )
        ],
        timestamp=TimestampFeature(),
    ).with_windows(definitions=["1 week", "2 days"])


@fixture
def expected_schema():
    return [
        {"column_name": "id", "type": DataType.BIGINT.cassandra, "primary_key": True},
        {
            "column_name": "timestamp",
            "type": DataType.TIMESTAMP.cassandra,
            "primary_key": False,
        },
        {
            "column_name": "feature1__avg_over_1_week_rolling_windows",
            "type": DataType.DOUBLE.cassandra,
            "primary_key": False,
        },
        {
            "column_name": "feature1__avg_over_2_days_rolling_windows",
            "type": DataType.DOUBLE.cassandra,
            "primary_key": False,
        },
        {
            "column_name": "feature1__stddev_pop_over_1_week_rolling_windows",
            "type": DataType.DOUBLE.cassandra,
            "primary_key": False,
        },
        {
            "column_name": "feature1__stddev_pop_over_2_days_rolling_windows",
            "type": DataType.DOUBLE.cassandra,
            "primary_key": False,
        },
        {
            "column_name": "feature2__count_over_1_week_rolling_windows",
            "type": DataType.INTEGER.cassandra,
            "primary_key": False,
        },
        {
            "column_name": "feature2__count_over_2_days_rolling_windows",
            "type": DataType.INTEGER.cassandra,
            "primary_key": False,
        },
    ]
