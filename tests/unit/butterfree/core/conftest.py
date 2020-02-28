import json

from pyspark.sql.types import StringType, StructField, StructType
from pytest import fixture

from butterfree.core.constants.columns import TIMESTAMP_COLUMN
from butterfree.core.constants.data_type import DataType


@fixture
def target_df(spark_context, spark_session):
    data = [
        {"id": 1, TIMESTAMP_COLUMN: 1460374271, "feature1": 100, "feature2": 100},
        {"id": 2, TIMESTAMP_COLUMN: 1460375052, "feature1": 200, "feature2": 200},
        {"id": 3, TIMESTAMP_COLUMN: 1460375184, "feature1": 300, "feature2": 300},
        {"id": 4, TIMESTAMP_COLUMN: 1460376201, "feature1": 400, "feature2": 400},
    ]
    df = spark_session.read.json(
        spark_context.parallelize(data).map(lambda x: json.dumps(x))
    )
    df = df.withColumn(TIMESTAMP_COLUMN, df.timestamp.cast(DataType.TIMESTAMP.value))
    return df


@fixture
def target_df_without_ts(spark_context, spark_session):
    data = [
        {"id": 1, "feature": 100},
        {"id": 2, "feature": 200},
        {"id": 3, "feature": 110},
        {"id": 4, "feature": 120},
    ]
    return spark_session.read.json(
        spark_context.parallelize(data).map(lambda x: json.dumps(x))
    )


@fixture
def target_df_empty(spark_context, spark_session):

    field = [StructField("field1", StringType(), True)]
    schema = StructType(field)

    return spark_session.createDataFrame(spark_context.emptyRDD(), schema)
