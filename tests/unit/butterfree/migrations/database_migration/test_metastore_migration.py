from butterfree.migrations.database_migration import MetastoreMigration


class TestMetastoreMigration:
    def test_queries(self, fs_schema, db_schema):
        metastore_migration = MetastoreMigration()

        expected_query = [
            "ALTER TABLE test.table_name ADD IF NOT EXISTS "
            "columns (new_feature FloatType);",
            "ALTER TABLE table_name DROP IF EXISTS "
            "(feature1__avg_over_2_days_rolling_windows None);",
            "ALTER TABLE table_name ALTER COLUMN "
            "(feature1__avg_over_1_week_rolling_windows FloatType);",
        ]

        query = metastore_migration.create_query(fs_schema, "table_name", db_schema)

        assert query, expected_query

    def test_queries_on_entity(self, fs_schema, db_schema):
        metastore_migration = MetastoreMigration()

        expected_query = [
            "ALTER TABLE test.table_name ADD IF NOT EXISTS "
            "columns (new_feature FloatType);",
            "ALTER TABLE table_name ALTER COLUMN "
            "(feature1__avg_over_1_week_rolling_windows FloatType);",
        ]

        query = metastore_migration.create_query(
            fs_schema, "table_name", db_schema, True
        )

        assert query, expected_query

    def test_create_table_query(self, fs_schema):

        metastore_migration = MetastoreMigration()

        expected_query = [
            "CREATE TABLE IF NOT EXISTS  test.table_name "
            "(id LongType, timestamp TimestampType, new_feature FloatType) "
            "PARTITIONED BY (year INT, month INT, day INT);"
        ]

        query = metastore_migration.create_query(fs_schema, "table_name")

        assert query, expected_query
