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
        {
            "id": 1,
            "ts": "2019-12-01",
            "feature": 100,
            "partition__year": 2019,
            "partition__month": 12,
            "partition__day": 1,
        },
        {
            "id": 2,
            "ts": "2020-01-01",
            "feature": 200,
            "partition__year": 2020,
            "partition__month": 1,
            "partition__day": 1,
        },
        {
            "id": 1,
            "ts": "2020-02-01",
            "feature": 110,
            "partition__year": 2020,
            "partition__month": 2,
            "partition__day": 1,
        },
        {
            "id": 1,
            "ts": "2020-02-02",
            "feature": 120,
            "partition__year": 2020,
            "partition__month": 2,
            "partition__day": 2,
        },
    ]
    return spark.read.json(sc.parallelize(data, 1))
