from pyspark.sql.types import DoubleType, FloatType, LongType, TimestampType

from butterfree.migrations.database_migration import CassandraMigration, Diff


class TestDatabaseMigration:
    def test__get_diff_empty(self, mocker, db_schema):
        fs_schema = [
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
        m = CassandraMigration()
        m._client = mocker.stub("client")
        diff = m._get_diff(fs_schema, db_schema)
        assert not diff

    def test__get_diff(self, mocker, db_schema):
        fs_schema = [
            {"column_name": "id", "type": LongType(), "primary_key": True},
            {"column_name": "timestamp", "type": TimestampType(), "primary_key": True},
            {"column_name": "new_feature", "type": FloatType(), "primary_key": False},
            {
                "column_name": "feature1__avg_over_1_week_rolling_windows",
                "type": FloatType(),
                "primary_key": False,
            },
        ]
        expected_diff = {
            Diff("timestamp", kind=Diff.Kind.ALTER_KEY, value=None),
            Diff("new_feature", kind=Diff.Kind.ADD, value=FloatType()),
            Diff(
                "feature1__avg_over_2_days_rolling_windows",
                kind=Diff.Kind.DROP,
                value=None,
            ),
            Diff(
                "feature1__avg_over_1_week_rolling_windows",
                kind=Diff.Kind.ALTER_TYPE,
                value=FloatType(),
            ),
        }

        m = CassandraMigration()
        m._client = mocker.stub("client")
        diff = m._get_diff(fs_schema, db_schema)
        assert diff == expected_diff

    def test_apply_migration(self, feature_set, mocker):
        # given
        m = CassandraMigration()
        m.apply_migration = mocker.stub("apply_migration")

        # when
        m.apply_migration(feature_set)

        # then
        m.apply_migration.assert_called_once()
