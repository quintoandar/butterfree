import pytest
from pyspark.sql.functions import col, from_unixtime

from butterfree.testing.dataframe import assert_dataframe_equality


def test_assert_dataframe_equality(spark_context, spark_session):
    # arrange
    data1 = [
        {"ts": 1582911000000, "flag": 1, "value": 1234.0},
        {"ts": 1577923200000, "flag": 0, "value": 123.0},
    ]
    data2 = [
        {"ts": "2020-01-02T00:00:00+00:00", "flag": "false", "value": 123},
        {"ts": "2020-02-28T17:30:00+00:00", "flag": "true", "value": 1234},
    ]  # same data declared in different formats and in different order

    df1 = spark_session.read.json(spark_context.parallelize(data1, 1))
    df1 = (
        df1.withColumn("ts", from_unixtime(col("ts") / 1000.0).cast("timestamp"))
        .withColumn("flag", col("flag").cast("boolean"))
        .withColumn("value", col("flag").cast("integer"))
    )

    df2 = spark_session.read.json(spark_context.parallelize(data2, 1))
    df2 = (
        df2.withColumn("ts", col("ts").cast("timestamp"))
        .withColumn("flag", col("flag").cast("boolean"))
        .withColumn("value", col("flag").cast("integer"))
    )

    # act and assert
    assert_dataframe_equality(df1, df2)


def test_assert_dataframe_equality_different_values(spark_context, spark_session):
    # arrange
    data1 = [
        {"value": "abc"},
        {"value": "cba"},
    ]
    data2 = [
        {"value": "abc"},
        {"value": "different value"},
    ]

    df1 = spark_session.read.json(spark_context.parallelize(data1, 1))
    df2 = spark_session.read.json(spark_context.parallelize(data2, 1))

    # act and assert
    with pytest.raises(AssertionError, match="DataFrames have different values:"):
        assert_dataframe_equality(df1, df2)


def test_assert_dataframe_equality_different_shapes(spark_context, spark_session):
    # arrange
    data1 = [
        {"value": "abc"},
        {"value": "cba"},
        {"value": "cba"},
    ]
    data2 = [
        {"value": "abc"},
        {"value": "cba"},
    ]

    df1 = spark_session.read.json(spark_context.parallelize(data1, 1))
    df2 = spark_session.read.json(spark_context.parallelize(data2, 1))

    # act and assert
    with pytest.raises(AssertionError, match="DataFrame shape mismatch:"):
        assert_dataframe_equality(df1, df2)
