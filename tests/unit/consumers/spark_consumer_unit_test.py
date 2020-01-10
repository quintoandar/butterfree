import pytest

from butterfree.core.clients.spark_client import SparkClient
from butterfree.core.consumers.spark_consumer import SparkConsumer

from unittest.mock import Mock

from pyspark.sql import session
from pyspark import SparkContext


def base_spark():
    sc = SparkContext.getOrCreate()
    spark = session.SparkSession(sc)

    return sc, spark


def test_spark_consumer():
    with pytest.raises(ValueError):
        spark_client = SparkClient()
        spark_consumer = SparkConsumer(spark_client)
        options = {"database": "a", "table_name": "b"}
        spark_consumer.consume(options)


def test_spark_consumer_get_table_schema():

    sc, spark = base_spark()
    data = [{"col_name": "test_data", "data_type": "int"}]
    df = spark.read.json(sc.parallelize(data, 1))

    spark_client = Mock()
    spark_client.get_records.return_value = df
    spark_consumer = SparkConsumer(spark_client)

    options = {"db": "a", "table_name": "b"}
    df = spark_consumer.consume(options)

    assert all([a == b for a, b in zip(df.columns, ["col_name", "col_type"])])


def test_spark_consumer_get_data_from_query():

    sc, spark = base_spark()
    data = [{"a": 1, "b": 2}]
    df = spark.read.json(sc.parallelize(data, 1))

    spark_client = Mock()
    spark_client.get_records.return_value = df
    spark_consumer = SparkConsumer(spark_client)

    options = {"query": "select * from db.table", "db": "db"}
    result_df = spark_consumer.consume(options)

    assert type(result_df) == type(df)
