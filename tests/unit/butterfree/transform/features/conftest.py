from unittest.mock import Mock

from pytest import fixture

from butterfree.constants.columns import TIMESTAMP_COLUMN
from butterfree.transform.features import Feature


@fixture
def feature_set_dataframe(spark_context, spark_session):
    data = [
        {"id": 1, TIMESTAMP_COLUMN: 0, "feature": 100},
        {"id": 2, TIMESTAMP_COLUMN: 1, "feature": 200},
    ]
    return spark_session.read.json(spark_context.parallelize(data, 1))


@fixture
def feature_set_dataframe_ms_from_column(spark_context, spark_session):
    data = [
        {"id": 1, "ts": 1581542311000, "feature": 100},
        {"id": 2, "ts": 1581542322000, "feature": 200},
    ]
    return spark_session.read.json(spark_context.parallelize(data, 1))


@fixture
def feature_set_dataframe_ms(spark_context, spark_session):
    data = [
        {"id": 1, TIMESTAMP_COLUMN: 1581542311000, "feature": 100},
        {"id": 2, TIMESTAMP_COLUMN: 1581542322000, "feature": 200},
    ]
    return spark_session.read.json(spark_context.parallelize(data, 1))


@fixture
def feature_set_dataframe_date(spark_context, spark_session):
    data = [
        {"id": 1, TIMESTAMP_COLUMN: "2020-02-07T00:00:00", "feature": 100},
        {"id": 2, TIMESTAMP_COLUMN: "2020-02-08T00:00:00", "feature": 200},
    ]
    return spark_session.read.json(spark_context.parallelize(data, 1))


@fixture
def mocked_feature():
    return Mock(spec=Feature)
