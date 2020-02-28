import json

from pytest import fixture

from butterfree.core.constants.columns import TIMESTAMP_COLUMN
from butterfree.core.constants.data_type import DataType


@fixture
def input_ms_from_column(spark_context, spark_session):
    data = [
        {"id": 1, "ts": 1460374271000, "feature1": 100, "feature2": 100},
        {"id": 2, "ts": 1460375052000, "feature1": 200, "feature2": 200},
        {"id": 3, "ts": 1460375184000, "feature1": 300, "feature2": 300},
        {"id": 4, "ts": 1460376201000, "feature1": 400, "feature2": 400},
    ]
    return spark_session.read.json(
        spark_context.parallelize(data).map(lambda x: json.dumps(x))
    )


@fixture
def input_ms(spark_context, spark_session):
    data = [
        {"id": 1, TIMESTAMP_COLUMN: 1460374271000, "feature1": 100, "feature2": 100},
        {"id": 2, TIMESTAMP_COLUMN: 1460375052000, "feature1": 200, "feature2": 200},
        {"id": 3, TIMESTAMP_COLUMN: 1460375184000, "feature1": 300, "feature2": 300},
        {"id": 4, TIMESTAMP_COLUMN: 1460376201000, "feature1": 400, "feature2": 400},
    ]
    return spark_session.read.json(
        spark_context.parallelize(data).map(lambda x: json.dumps(x))
    )


@fixture
def input_date(spark_context, spark_session):
    data = [
        {"id": 1, TIMESTAMP_COLUMN: "2019-02-12", "feature": 100},
        {"id": 2, TIMESTAMP_COLUMN: "2019-02-12", "feature": 200},
    ]
    return spark_session.read.json(
        spark_context.parallelize(data).map(lambda x: json.dumps(x))
    )


@fixture
def date_target_df(spark_context, spark_session):
    data = [
        {"id": 1, TIMESTAMP_COLUMN: "2019-02-12 00:00:00", "feature": 100},
        {"id": 2, TIMESTAMP_COLUMN: "2019-02-12 00:00:00", "feature": 200},
    ]
    df = spark_session.read.json(
        spark_context.parallelize(data).map(lambda x: json.dumps(x))
    )
    df = df.withColumn(TIMESTAMP_COLUMN, df.timestamp.cast(DataType.TIMESTAMP.value))
    return df
