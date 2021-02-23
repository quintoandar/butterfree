from butterfree.migrations import CassandraMigration


class TestCassandraMigration:
    def test_alter_table_query(self, dummy_db_schema, dummy_schema_diff):
        cassandra_migration = CassandraMigration()

        expected_query = "ALTER TABLE test ADD (kappa_column int, pogchamp uuid);"
        query = cassandra_migration.create_query(
            "test", dummy_db_schema, dummy_schema_diff
        )

        assert isinstance(query, str)
        assert query, expected_query

    def test_create_table_query(self, dummy_db_schema):

        cassandra_migration = CassandraMigration()

        expected_query = (
            "CREATE TABLE test.test_table "
            "(id int, platform text, ts bigint, PRIMARY KEY (id));"
        )
        query = cassandra_migration.create_query(
            table_name="test_table", schema_diff=dummy_db_schema
        )

        assert isinstance(query, str)
        assert query, expected_query
