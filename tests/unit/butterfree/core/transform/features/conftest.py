from unittest.mock import Mock

from pyspark import SparkContext
from pyspark.sql import session
from pytest import fixture

from butterfree.core.constants.columns import TIMESTAMP_COLUMN
from butterfree.core.transform.features import Feature


@fixture
def base_spark():
    sc = SparkContext.getOrCreate()
    spark = session.SparkSession(sc).builder.config("spark.sql.session.timeZone", "UTC").getOrCreate()

    return sc, spark


@fixture
def feature_set_dataframe(base_spark):
    sc, spark = base_spark
    data = [
        {"id": 1, TIMESTAMP_COLUMN: 0, "feature": 100},
        {"id": 2, TIMESTAMP_COLUMN: 1, "feature": 200},
    ]
    return spark.read.json(sc.parallelize(data, 1))


@fixture
def feature_set_dataframe_ms_from_column(base_spark):
    sc, spark = base_spark
    data = [
        {"id": 1, "ts": 1581542311000, "feature": 100},
        {"id": 2, "ts": 1581542322000, "feature": 200},
    ]
    return spark.read.json(sc.parallelize(data, 1))


@fixture
def feature_set_dataframe_ms(base_spark):
    sc, spark = base_spark
    data = [
        {"id": 1, TIMESTAMP_COLUMN: 1581542311000, "feature": 100},
        {"id": 2, TIMESTAMP_COLUMN: 1581542322000, "feature": 200},
    ]
    return spark.read.json(sc.parallelize(data, 1))


@fixture
def feature_set_dataframe_date(base_spark):
    sc, spark = base_spark
    data = [
        {"id": 1, TIMESTAMP_COLUMN: "2020-02-07 00:00:00", "feature": 100},
        {"id": 2, TIMESTAMP_COLUMN: "2020-02-08 00:00:00", "feature": 200},
    ]
    return spark.read.json(sc.parallelize(data, 1))


@fixture
def mocked_feature():
    return Mock(spec=Feature)
