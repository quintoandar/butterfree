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
        {"id": 1, "feature1": 200, "feature2": 200},
        {"id": 1, "feature1": 300, "feature2": 300},
        {"id": 1, "feature1": 400, "feature2": 400},
        {"id": 1, "feature1": 500, "feature2": 500},
    ]
    return spark.read.json(sc.parallelize(data, 1))
