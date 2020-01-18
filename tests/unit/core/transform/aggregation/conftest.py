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
        {"id": 1, "ts": "2016-04-11 19:01:00", "feature": 100},
        {"id": 1, "ts": "2016-04-12 19:12:00", "feature": 200},
        {"id": 1, "ts": "2016-04-13 19:13:00", "feature": 200},
        {"id": 1, "ts": "2016-04-14 19:24:00", "feature": 200},
        {"id": 1, "ts": "2016-04-14 19:28:00", "feature": 200},
        {"id": 1, "ts": "2016-04-14 19:30:00", "feature": 200},
        {"id": 1, "ts": "2016-04-14 19:45:00", "feature": 200},
        {"id": 1, "ts": "2016-04-14 19:48:00", "feature": 200},
        {"id": 1, "ts": "2016-04-17 19:50:00", "feature": 200},
        {"id": 1, "ts": "2016-04-18 19:59:00", "feature": 200},
        {"id": 1, "ts": "2016-04-18 20:02:00", "feature": 200},
        {"id": 1, "ts": "2016-04-18 20:15:00", "feature": 200},
    ]
    df = spark.read.json(sc.parallelize(data, 1))
    df = df.withColumn("timestamp", df.ts.cast("timestamp"))

    return df
