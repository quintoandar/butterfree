from unittest.mock import Mock

import pytest

from butterfree.constants.columns import TIMESTAMP_COLUMN


@pytest.fixture()
def column_target_df(spark_context, spark_session):
    data = [{"new_col1": "value", "new_col2": 123}]
    return spark_session.read.json(spark_context.parallelize(data, 1))


@pytest.fixture()
def target_df(spark_context, spark_session):
    data = [{"col1": "value", "col2": 123}]
    return spark_session.read.json(spark_context.parallelize(data, 1))


@pytest.fixture()
def spark_client():
    return Mock()


@pytest.fixture
def feature_set_dataframe(spark_context, spark_session):
    data = [
        {"id": 1, TIMESTAMP_COLUMN: 0, "feature": 100, "test": "fail"},
        {"id": 2, TIMESTAMP_COLUMN: 0, "feature": 200, "test": "running"},
        {"id": 1, TIMESTAMP_COLUMN: 1, "feature": 110, "test": "pass"},
        {"id": 1, TIMESTAMP_COLUMN: 2, "feature": 120, "test": "pass"},
    ]
    return spark_session.read.json(spark_context.parallelize(data, 1))
