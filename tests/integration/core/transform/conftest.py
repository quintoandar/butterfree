from pyspark import SparkContext
from pyspark.sql import session
from pytest import fixture

from butterfree.core.constant.columns import TIMESTAMP_COLUMN
from butterfree.core.constant.data_type import DataType


def base_spark():
    sc = SparkContext.getOrCreate()
    spark = session.SparkSession(sc)

    return sc, spark


@fixture
def feature_set_dataframe():
    sc, spark = base_spark()
    data = [
        {"id": 1, "origin_ts": "2016-04-11 11:31:11", "feature1": 200, "feature2": 200},
        {"id": 1, "origin_ts": "2016-04-11 11:44:12", "feature1": 300, "feature2": 300},
        {"id": 1, "origin_ts": "2016-04-11 11:46:24", "feature1": 400, "feature2": 400},
        {"id": 1, "origin_ts": "2016-04-11 12:03:21", "feature1": 500, "feature2": 500},
    ]
    df = spark.read.json(sc.parallelize(data, 1))
    df = df.withColumn(TIMESTAMP_COLUMN, df.origin_ts.cast(DataType.TIMESTAMP.value))

    return df
