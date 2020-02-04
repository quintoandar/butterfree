from pyspark import SparkContext
from pyspark.sql import session
from pyspark.sql.functions import col
from pyspark.sql.types import StringType, StructField, StructType
from pytest import fixture

from butterfree.core.constant.columns import TIMESTAMP_COLUMN
from butterfree.core.constant.data_type import DataType
from butterfree.core.db.configs import CassandraConfig


def base_spark():
    sc = SparkContext.getOrCreate()
    spark = (
        session.SparkSession(sc)
        .builder.config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )

    return sc, spark


@fixture
def feature_set_dataframe():
    sc, spark = base_spark()
    data = [
        {"id": 1, TIMESTAMP_COLUMN: 0, "feature": 100},
        {"id": 2, TIMESTAMP_COLUMN: 0, "feature": 200},
        {"id": 1, TIMESTAMP_COLUMN: 1, "feature": 110},
        {"id": 1, TIMESTAMP_COLUMN: 2, "feature": 120},
    ]
    return spark.read.json(sc.parallelize(data, 1))


@fixture
def input_feature_set_dataframe():
    sc, spark = base_spark()
    data = [
        {"id": 1, TIMESTAMP_COLUMN: 0, "feature": 100},
        {"id": 2, TIMESTAMP_COLUMN: 0, "feature": 200},
        {"id": 1, TIMESTAMP_COLUMN: 1, "feature": 110},
        {"id": 1, TIMESTAMP_COLUMN: 2, "feature": 120},
    ]
    return spark.read.json(sc.parallelize(data, 1)).withColumn(
        TIMESTAMP_COLUMN, col(TIMESTAMP_COLUMN).cast(DataType.TIMESTAMP.value)
    )


@fixture
def output_feature_set_dataframe():
    sc, spark = base_spark()
    data = [
        {
            "feature": 100,
            "id": 1,
            TIMESTAMP_COLUMN: 0,
            "partition__year": 1970,
            "partition__month": 1,
            "partition__day": 1,
        },
        {
            "id": 2,
            TIMESTAMP_COLUMN: 0,
            "feature": 200,
            "partition__year": 1970,
            "partition__month": 1,
            "partition__day": 1,
        },
        {
            "id": 1,
            TIMESTAMP_COLUMN: 1,
            "feature": 110,
            "partition__year": 1970,
            "partition__month": 1,
            "partition__day": 1,
        },
        {
            "id": 1,
            TIMESTAMP_COLUMN: 2,
            "feature": 120,
            "partition__year": 1970,
            "partition__month": 1,
            "partition__day": 1,
        },
    ]
    return spark.read.json(sc.parallelize(data, 1)).withColumn(
        TIMESTAMP_COLUMN, col(TIMESTAMP_COLUMN).cast(DataType.TIMESTAMP.value)
    )


@fixture
def feature_set_count_dataframe():
    sc, spark = base_spark()
    data = [
        {"row": 4},
    ]
    return spark.read.json(sc.parallelize(data, 1))


@fixture
def latest():
    sc, spark = base_spark()
    data = [
        {"id": 2, TIMESTAMP_COLUMN: 0, "feature": 200},
        {"id": 1, TIMESTAMP_COLUMN: 2, "feature": 120},
    ]
    return spark.read.json(sc.parallelize(data, 1))


@fixture
def feature_set_without_ts():
    sc, spark = base_spark()
    data = [
        {"id": 1, "feature": 100},
        {"id": 2, "feature": 200},
        {"id": 1, "feature": 110},
        {"id": 1, "feature": 120},
    ]
    return spark.read.json(sc.parallelize(data, 1))


@fixture
def feature_set_not_df():
    data = "not a spark df writer"
    return data


@fixture
def feature_set_empty():
    sc, spark = base_spark()

    field = [StructField("field1", StringType(), True)]
    schema = StructType(field)

    return spark.createDataFrame(sc.emptyRDD(), schema)


@fixture
def cassandra_config():
    return CassandraConfig(keyspace="test")


@fixture(params=["feature_set_empty", "feature_set_without_ts", "feature_set_not_df"])
def feature_sets(request):
    return request.getfixturevalue(request.param)
