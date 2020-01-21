from pyspark import SparkContext
from pyspark.sql import session
from pyspark.sql.types import StringType, StructField, StructType
from pytest import fixture

from butterfree.core.db.configs import CassandraWriteConfig


def base_spark():
    sc = SparkContext.getOrCreate()
    spark = session.SparkSession(sc)

    return sc, spark


@fixture
def feature_set_dataframe():
    sc, spark = base_spark()
    data = [
        {"id": 1, "ts": 0, "feature": 100},
        {"id": 2, "ts": 0, "feature": 200},
        {"id": 1, "ts": 1, "feature": 110},
        {"id": 1, "ts": 2, "feature": 120},
    ]
    return spark.read.json(sc.parallelize(data, 1))


@fixture
def latest():
    sc, spark = base_spark()
    data = [
        {"id": 2, "ts": 0, "feature": 200},
        {"id": 1, "ts": 2, "feature": 120},
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
def feature_set_nullable():
    sc, spark = base_spark()

    field = [StructField("field1", StringType(), True)]
    schema = StructType(field)

    return spark.createDataFrame(sc.emptyRDD(), schema)


@fixture
def cassandra_config():
    return CassandraWriteConfig(keyspace="test")
