from pyspark.sql import Row, SparkSession


def create_data_mock(spark: SparkSession):
    data_mock = [
        Row(a=1, b="a", c="1",),
        Row(a=2, b="b", c="2",),
        Row(a=3, b="c", c="3",),
        Row(a=4, b="d", c="4",),
        Row(a=5, b="e", c="5",),
    ]

    return spark.createDataFrame(data_mock)
