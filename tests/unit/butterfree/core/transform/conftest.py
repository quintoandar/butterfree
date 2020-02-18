from unittest.mock import Mock

from pyspark import SparkContext
from pyspark.sql import session
from pytest import fixture

from butterfree.core.constants.columns import TIMESTAMP_COLUMN
from butterfree.core.constants.data_type import DataType
from butterfree.core.transform.features import Feature, KeyFeature, TimestampFeature


def base_spark():
    sc = SparkContext.getOrCreate()
    spark = session.SparkSession(sc)

    return sc, spark


def make_dataframe():
    sc, spark = base_spark()
    data = [
        {
            "id": 1,
            "ts": "2016-04-11 11:31:11",
            "feature1": 200,
            "feature2": 200,
            "nonfeature": 0,
        },
        {
            "id": 1,
            "ts": "2016-04-11 11:44:12",
            "feature1": 300,
            "feature2": 300,
            "nonfeature": 0,
        },
        {
            "id": 1,
            "ts": "2016-04-11 11:46:24",
            "feature1": 400,
            "feature2": 400,
            "nonfeature": 0,
        },
        {
            "id": 1,
            "ts": "2016-04-11 12:03:21",
            "feature1": 500,
            "feature2": 500,
            "nonfeature": 0,
        },
    ]
    df = spark.read.json(sc.parallelize(data, 1))
    df = df.withColumn(TIMESTAMP_COLUMN, df.ts.cast(DataType.TIMESTAMP.value))

    return df


def make_fs():
    df = make_dataframe()
    return (
        df.withColumn("add", df.feature1 + df.feature2)
        .withColumn("divide", df.feature1 / df.feature2)
        .select("id", TIMESTAMP_COLUMN, "add", "divide")
    )


@fixture
def dataframe():
    return make_dataframe()


@fixture
def feature_set_dataframe():
    return make_fs()


@fixture
def feature_add():
    fadd = Mock(spec=Feature)
    fadd.get_output_columns = Mock(return_value=["add"])
    fadd.transform = Mock(return_value=make_fs())
    return fadd


@fixture
def feature_divide():
    fdivide = Mock(spec=Feature)
    fdivide.get_output_columns = Mock(return_value=["divide"])
    fdivide.transform = Mock(return_value=make_fs())
    return fdivide


@fixture
def key_id():
    return KeyFeature(name="id", description="description")


@fixture
def timestamp_c():
    return TimestampFeature()
