from butterfree.migrations.database_migration import CassandraMigration


class TestDatabaseMigration:
    def test_validate_schema(self, mocker, db_schema):
        fs_schema = [
            {"column_name": "id", "type": "bigint", "primary_key": True},
            {"column_name": "timestamp", "type": "timestamp", "primary_key": False},
        ]

        m = CassandraMigration()
        m._client = mocker.stub("client")
        schema = m._get_diff(fs_schema, db_schema)
        assert not schema

    def test_validate_schema_diff(self, mocker, db_schema):
        fs_schema = [
            {"column_name": "id", "type": "bigint", "primary_key": True},
            {"column_name": "timestamp", "type": "timestamp", "primary_key": False},
            {"column_name": "new_feature", "type": "float", "primary_key": False},
        ]

        m = CassandraMigration()
        m._client = mocker.stub("client")
        schema = m._get_diff(fs_schema, db_schema)
        assert schema == [
            {"column_name": "new_feature", "type": "float", "primary_key": False},
        ]

    def test_validate_schema_diff_invalid(self, mocker, db_schema):
        schema_diff = [
            {
                "column_name": "feature1__avg_over_1_week_rolling_windows",
                "type": "float",
                "primary_key": False,
            },
            {"column_name": "new_feature", "type": "float", "primary_key": False},
        ]

        m = CassandraMigration()
        m._client = mocker.stub("client")

        inconsistent_features = m._get_type_inconsistent_features(
            schema_diff, db_schema
        )

        assert inconsistent_features == [
            {
                "column_name": "feature1__avg_over_1_week_rolling_windows",
                "type": "float",
                "primary_key": False,
            },
        ]

    def test_validate_schema_without_db(self, mocker):
        fs_schema = [
            {"column_name": "id", "type": "bigint", "primary_key": True},
            {"column_name": "timestamp", "type": "timestamp", "primary_key": False},
            {
                "column_name": "feature1__avg_over_1_week_rolling_windows",
                "type": "float",
                "primary_key": False,
            },
        ]

        db_schema = None

        m = CassandraMigration()
        m._client = mocker.stub("client")
        schema = m._get_diff(fs_schema, db_schema)
        assert schema == [
            {"column_name": "id", "type": "bigint", "primary_key": True},
            {"column_name": "timestamp", "type": "timestamp", "primary_key": False},
            {
                "column_name": "feature1__avg_over_1_week_rolling_windows",
                "type": "float",
                "primary_key": False,
            },
        ]

    def test_apply_migration(self, feature_set, mocker):
        # given
        m = CassandraMigration()
        m.apply_migration = mocker.stub("apply_migration")

        # when
        m.apply_migration(feature_set)

        # then
        m.apply_migration.assert_called_once()
