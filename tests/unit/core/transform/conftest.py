from unittest.mock import Mock

from pyspark import SparkContext
from pyspark.sql import session
from pytest import fixture

from butterfree.core.transform import Feature

@fixture
def base_spark():
    sc = SparkContext.getOrCreate()
    spark = session.SparkSession(sc)

    return sc, spark


@fixture
def feature_set_dataframe():
    sc, spark = base_spark()
    data = [
        {"id": 1, "ts": 0, "feature": 100},
        {"id": 2, "ts": 1, "feature": 200},
    ]
    return spark.read.json(sc.parallelize(data, 1))


@fixture
def mocked_feature():
    return Mock(spec=Feature)
