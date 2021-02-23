from pyspark.sql.types import DoubleType, LongType, TimestampType
from pytest import fixture


@fixture
def db_schema():
    return [
        {"column_name": "id", "type": LongType(), "primary_key": True},
        {"column_name": "timestamp", "type": TimestampType(), "primary_key": False},
        {
            "column_name": "feature1__avg_over_1_week_rolling_windows",
            "type": DoubleType(),
            "primary_key": False,
        },
        {
            "column_name": "feature1__avg_over_2_days_rolling_windows",
            "type": DoubleType(),
            "primary_key": False,
        },
    ]
