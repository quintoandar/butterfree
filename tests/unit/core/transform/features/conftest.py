from unittest.mock import Mock

from pyspark import SparkContext
from pyspark.sql import session
from pytest import fixture

from butterfree.core.constant.columns import TIMESTAMP_COLUMN
from butterfree.core.transform.features import Feature


@fixture
def base_spark():
    sc = SparkContext.getOrCreate()
    spark = session.SparkSession(sc)

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
def mocked_feature():
    return Mock(spec=Feature)
