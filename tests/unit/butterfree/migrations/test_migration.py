from pyspark.sql.types import LongType, TimestampType, FloatType, DoubleType

from butterfree.migrations import Migration


class TestMigration:
    def test_validate_schema(self):
        fs_schema = [
            {"column_name": "id", "type": LongType(), "primary_key": True},
            {"column_name": "timestamp", "type": TimestampType(), "primary_key": False},
            {
                "column_name": "feature1__avg_over_2_minutes_fixed_windows",
                "type": FloatType(),
                "primary_key": False,
            },
        ]

        db_schema = [
            {"column_name": "id", "type": LongType(), "primary_key": True},
            {"column_name": "timestamp", "type": TimestampType(), "primary_key": False},
            {
                "column_name": "feature1__avg_over_2_minutes_fixed_windows",
                "type": FloatType(),
                "primary_key": False,
            },
            {
                "column_name": "feature1__avg_over_15_minutes_fixed_windows",
                "type": FloatType(),
                "primary_key": False,
            },
            {
                "column_name": "feature1__stddev_pop_over_2_minutes_fixed_windows",
                "type": DoubleType(),
                "primary_key": False,
            },
            {
                "column_name": "feature1__stddev_pop_over_15_minutes_fixed_windows",
                "type": DoubleType(),
                "primary_key": False,
            },
        ]

        Migration.validate_schema(fs_schema, db_schema)

