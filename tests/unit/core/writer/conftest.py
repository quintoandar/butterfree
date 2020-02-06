from pyspark import SparkContext
from pyspark.sql import session
from pyspark.sql.types import StringType, StructField, StructType
from pytest import fixture

from butterfree.core.constant.columns import TIMESTAMP_COLUMN
from butterfree.core.db.configs import CassandraConfig
from butterfree.core.transform import FeatureSet
from butterfree.core.transform.features import Feature, KeyFeature, TimestampFeature


def base_spark():
    sc = SparkContext.getOrCreate()
    spark = (
        session.SparkSession(sc)
        .builder.config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )

    return sc, spark


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
def feature_set_dataframe():
    sc, spark = base_spark()
    data = [
        {"id": 1, TIMESTAMP_COLUMN: "2019-12-31", "feature": 100},
        {"id": 2, TIMESTAMP_COLUMN: "2019-12-31", "feature": 200},
        {"id": 1, TIMESTAMP_COLUMN: "2020-01-15", "feature": 110},
        {"id": 1, TIMESTAMP_COLUMN: "2020-02-01", "feature": 120},
    ]
    return spark.read.json(sc.parallelize(data, 1))


@fixture
def historical_feature_set_dataframe():
    sc, spark = base_spark()
    data = [
        {
            "feature": 100,
            "id": 1,
            TIMESTAMP_COLUMN: "2019-12-31",
            "partition__year": 2019,
            "partition__month": 12,
            "partition__day": 31,
        },
        {
            "id": 2,
            TIMESTAMP_COLUMN: "2019-12-31",
            "feature": 200,
            "partition__year": 2019,
            "partition__month": 12,
            "partition__day": 31,
        },
        {
            "id": 1,
            TIMESTAMP_COLUMN: "2020-01-15",
            "feature": 110,
            "partition__year": 2020,
            "partition__month": 1,
            "partition__day": 15,
        },
        {
            "id": 1,
            TIMESTAMP_COLUMN: "2020-02-01",
            "feature": 120,
            "partition__year": 2020,
            "partition__month": 2,
            "partition__day": 1,
        },
    ]
    return spark.read.json(sc.parallelize(data, 1))


@fixture
def latest_feature_set_dataframe():
    sc, spark = base_spark()
    data = [
        {"id": 2, TIMESTAMP_COLUMN: "2019-12-31", "feature": 200},
        {"id": 1, TIMESTAMP_COLUMN: "2020-02-01", "feature": 120},
    ]
    return spark.read.json(sc.parallelize(data, 1))


@fixture
def feature_set_dataframe_without_ts():
    sc, spark = base_spark()
    data = [
        {"id": 1, "feature": 100},
        {"id": 2, "feature": 200},
        {"id": 1, "feature": 110},
        {"id": 1, "feature": 120},
    ]
    return spark.read.json(sc.parallelize(data, 1))


@fixture
def count_feature_set_dataframe():
    sc, spark = base_spark()
    data = [
        {"row": 4},
    ]
    return spark.read.json(sc.parallelize(data, 1))


@fixture
def not_feature_set_dataframe():
    data = "not a spark df writer"
    return data


@fixture
def empty_feature_set_dataframe():
    sc, spark = base_spark()

    field = [StructField("field1", StringType(), True)]
    schema = StructType(field)

    return spark.createDataFrame(sc.emptyRDD(), schema)


@fixture
def cassandra_config():
    return CassandraConfig(keyspace="feature_set")


@fixture(params=["feature_set_empty", "feature_set_without_ts", "feature_set_not_df"])
def feature_sets(request):
    return request.getfixturevalue(request.param)
