from unittest.mock import Mock

import pytest
from pyspark.sql.functions import col, to_date

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
def incremental_source_df(spark_context, spark_session):
    data = [
        {
            "id": 1,
            "feature": 100,
            "date_str": "28/07/2020",
            "milliseconds": 1595894400000,
            "year": 2020,
            "month": 7,
            "day": 28,
        },
        {
            "id": 1,
            "feature": 110,
            "date_str": "29/07/2020",
            "milliseconds": 1595980800000,
            "year": 2020,
            "month": 7,
            "day": 29,
        },
        {
            "id": 1,
            "feature": 120,
            "date_str": "30/07/2020",
            "milliseconds": 1596067200000,
            "year": 2020,
            "month": 7,
            "day": 30,
        },
        {
            "id": 2,
            "feature": 150,
            "date_str": "31/07/2020",
            "milliseconds": 1596153600000,
            "year": 2020,
            "month": 7,
            "day": 31,
        },
        {
            "id": 2,
            "feature": 200,
            "date_str": "01/08/2020",
            "milliseconds": 1596240000000,
            "year": 2020,
            "month": 8,
            "day": 1,
        },
    ]
    return spark_session.read.json(spark_context.parallelize(data, 1)).withColumn(
        "date", to_date(col("date_str"), "dd/MM/yyyy")
    )


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
