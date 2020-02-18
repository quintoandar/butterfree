from pyspark import SparkContext
from pyspark.sql import session
from pytest import fixture

from butterfree.core.constants import columns
from butterfree.core.transform import FeatureSet
from butterfree.core.transform.features import Feature, KeyFeature, TimestampFeature


def base_spark():
    sc = SparkContext.getOrCreate()
    spark = session.SparkSession(sc)

    return sc, spark


@fixture
def input_dataframe():
    sc, spark = base_spark()
    data = [
        {
            "id": 1,
            "timestamp": "2019-12-01",
            "feature": 100,
            columns.PARTITION_YEAR: 2019,
            columns.PARTITION_MONTH: 12,
            columns.PARTITION_DAY: 1,
        },
        {
            "id": 2,
            "timestamp": "2020-01-01",
            "feature": 200,
            columns.PARTITION_YEAR: 2020,
            columns.PARTITION_MONTH: 1,
            columns.PARTITION_DAY: 1,
        },
        {
            "id": 1,
            "timestamp": "2020-02-01",
            "feature": 110,
            columns.PARTITION_YEAR: 2020,
            columns.PARTITION_MONTH: 2,
            columns.PARTITION_DAY: 1,
        },
        {
            "id": 1,
            "timestamp": "2020-02-02",
            "feature": 120,
            columns.PARTITION_YEAR: 2020,
            columns.PARTITION_MONTH: 2,
            columns.PARTITION_DAY: 2,
        },
    ]
    return spark.read.json(sc.parallelize(data, 1))


@fixture
def feature_set():
    key_features = [KeyFeature(name="id", description="Description")]
    ts_feature = TimestampFeature(from_column="timestamp")
    features = [
        Feature(name="feature", description="Description"),
    ]
    return FeatureSet(
        "test_sink_feature_set",
        "test_sink_entity",
        "description",
        keys=key_features,
        timestamp=ts_feature,
        features=features,
    )
