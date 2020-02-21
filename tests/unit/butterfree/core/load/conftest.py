from pyspark.sql.types import StringType, StructField, StructType
from pytest import fixture

from butterfree.core.configs.db import CassandraConfig
from butterfree.core.constants import columns
from butterfree.core.constants.columns import TIMESTAMP_COLUMN
from butterfree.core.transform import FeatureSet
from butterfree.core.transform.features import Feature, KeyFeature, TimestampFeature


@fixture
def feature_set():
    key_features = [KeyFeature(name="id", description="Description")]
    ts_feature = TimestampFeature(from_column=TIMESTAMP_COLUMN)
    features = [
        Feature(name="feature", description="Description"),
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
