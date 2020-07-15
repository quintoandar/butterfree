from pyspark.sql.types import StringType, StructField, StructType
from pytest import fixture

from butterfree.constants.columns import TIMESTAMP_COLUMN


@fixture
def feature_set_dataframe(spark_context, spark_session):
    data = [
        {"id": 1, TIMESTAMP_COLUMN: 0, "feature": 100},
        {"id": 2, TIMESTAMP_COLUMN: 0, "feature": 200},
        {"id": 1, TIMESTAMP_COLUMN: 1, "feature": 110},
        {"id": 1, TIMESTAMP_COLUMN: 2, "feature": 120},
    ]
    return spark_session.read.json(spark_context.parallelize(data, 1))


@fixture
def feature_set_without_ts(spark_context, spark_session):
    data = [
        {"id": 1, "feature": 100},
        {"id": 2, "feature": 200},
        {"id": 1, "feature": 110},
        {"id": 1, "feature": 120},
    ]
    return spark_session.read.json(spark_context.parallelize(data, 1))


@fixture
def feature_set_empty(spark_context, spark_session):

    field = [StructField("field1", StringType(), True)]
    schema = StructType(field)

    return spark_session.createDataFrame(spark_context.emptyRDD(), schema)
