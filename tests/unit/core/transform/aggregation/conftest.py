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
        {"id": 1, "ts": "2016-04-11 11:31:11", "feature": 200},
        {"id": 1, "ts": "2016-04-11 11:44:12", "feature": 300},
        {"id": 1, "ts": "2016-04-11 11:46:24", "feature": 400},
        {"id": 1, "ts": "2016-04-11 12:03:21", "feature": 500},
    ]
    df = spark.read.json(sc.parallelize(data, 1))
    df = df.withColumn("timestamp", df.ts.cast("timestamp"))

    return df
