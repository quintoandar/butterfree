from pyspark.sql.types import FloatType, LongType, TimestampType

from butterfree.migrations.database_migration import CassandraMigration


class TestDatabaseMigration:
    def test_validate_schema(self, mocker, db_schema):
        fs_schema = [
            {"column_name": "id", "type": LongType(), "primary_key": True},
            {"column_name": "timestamp", "type": TimestampType(), "primary_key": False},
        ]

        m = CassandraMigration()
        m._client = mocker.stub("client")
        schema = m._get_diff(fs_schema, db_schema)
        assert not schema

    def test_validate_schema_diff(self, mocker, db_schema):
        fs_schema = [
            {"column_name": "id", "type": LongType(), "primary_key": True},
            {"column_name": "timestamp", "type": TimestampType(), "primary_key": False},
            {"column_name": "new_feature", "type": FloatType(), "primary_key": False},
        ]

        m = CassandraMigration()
        m._client = mocker.stub("client")
        schema = m._get_diff(fs_schema, db_schema)
        assert schema == [
            {"column_name": "new_feature", "type": FloatType(), "primary_key": False},
        ]

    def test_validate_schema_diff_invalid(self, mocker, db_schema):
        schema_diff = [
            {
                "column_name": "feature1__avg_over_1_week_rolling_windows",
                "type": FloatType(),
                "primary_key": False,
            },
            {"column_name": "new_feature", "type": FloatType(), "primary_key": False},
        ]

        m = CassandraMigration()
        m._client = mocker.stub("client")

        inconsistent_features = m._get_type_inconsistent_features(
            schema_diff, db_schema
        )

        assert inconsistent_features == [
            {
                "column_name": "feature1__avg_over_1_week_rolling_windows",
                "type": FloatType(),
                "primary_key": False,
            },
        ]

    def test_validate_schema_without_db(self, mocker):
        fs_schema = [
            {"column_name": "id", "type": LongType(), "primary_key": True},
            {"column_name": "timestamp", "type": TimestampType(), "primary_key": False},
            {
                "column_name": "feature1__avg_over_1_week_rolling_windows",
                "type": FloatType(),
                "primary_key": False,
            },
        ]

        db_schema = None

        m = CassandraMigration()
        m._client = mocker.stub("client")
        schema = m._get_diff(fs_schema, db_schema)
        assert schema == [
            {"column_name": "id", "type": LongType(), "primary_key": True},
            {"column_name": "timestamp", "type": TimestampType(), "primary_key": False},
            {
                "column_name": "feature1__avg_over_1_week_rolling_windows",
                "type": FloatType(),
                "primary_key": False,
            },
        ]
