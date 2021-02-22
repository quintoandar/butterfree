from butterfree.migrations import MetastoreMigration


class TestMetastoreMigration:
    def test_alter_table_query(self, dummy_db_schema, dummy_schema_diff):
        metastore_migration = MetastoreMigration()

        expected_query = (
            "ALTER TABLE test ADD columns (kappa_column INT, pogchamp BOOLEAN);"
        )
        query = metastore_migration.create_query(
            "test", dummy_db_schema, dummy_schema_diff
        )

        assert isinstance(query, str)
        assert query, expected_query

    def test_create_table_query(self, dummy_db_schema):

        metastore_migration = MetastoreMigration()

        expected_query = (
            "CREATE TABLE test.test_table "
            "(id INT, platform STRING, ts BIGINT, year INT, month INT, day INT) "
            "PARTITIONED BY (year, month, day);"
        )
        query = metastore_migration.create_query(
            table_name="test_table", schema_diff=dummy_db_schema
        )

        assert isinstance(query, str)
        assert query, expected_query

    def test_no_diff(self, dummy_db_schema):

        metastore_migration = MetastoreMigration()

        query = metastore_migration.create_query(
            table_name="test_table", db_schema=dummy_db_schema,
        )

        assert query is None
