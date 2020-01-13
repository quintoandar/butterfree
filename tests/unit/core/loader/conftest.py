from pyspark import SparkContext
from pyspark.sql import session
from pytest import fixture


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
