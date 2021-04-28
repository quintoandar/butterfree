from butterfree.migrations.database_migration import CassandraMigration


class TestCassandraMigration:
    def test_queries(self, fs_schema, db_schema):
        cassandra_migration = CassandraMigration()
        expected_query = [
            "ALTER TABLE table_name ADD (new_feature FloatType);",
            "ALTER TABLE table_name DROP (feature1__avg_over_2_days_rolling_windows);",
            "ALTER TABLE table_name ALTER "
            "feature1__avg_over_1_week_rolling_windows TYPE FloatType;",
        ]
        query = cassandra_migration.create_query(fs_schema, "table_name", db_schema)

        assert query, expected_query

    def test_queries_on_entity(self, fs_schema, db_schema):
        cassandra_migration = CassandraMigration()
        expected_query = [
            "ALTER TABLE table_name ADD (new_feature FloatType);",
            "ALTER TABLE table_name ALTER "
            "feature1__avg_over_1_week_rolling_windows TYPE FloatType;",
        ]
        query = cassandra_migration.create_query(
            fs_schema, "table_name", db_schema, True
        )

        assert query, expected_query

    def test_create_table_query(self, fs_schema):

        cassandra_migration = CassandraMigration()
        expected_query = [
            "CREATE TABLE test.table_name "
            "(id LongType, timestamp TimestampType, new_feature FloatType, "
            "feature1__avg_over_1_week_rolling_windows FloatType, "
            "PRIMARY KEY (id, timestamp));"
        ]
        query = cassandra_migration.create_query(fs_schema, "table_name")

        assert query, expected_query
