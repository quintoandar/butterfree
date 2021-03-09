from pyspark.sql.types import FloatType

from butterfree.migrations.database_migration import CassandraMigration, Diff


class TestCassandraMigration:
    def test_queries(self):
        cassandra_migration = CassandraMigration()

        diff = {
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
        expected_query = [
            "ALTER TABLE table_name ADD (new_feature FloatType);",
            "ALTER TABLE table_name DROP (feature1__avg_over_2_days_rolling_windows);",
            "ALTER TABLE table_name ALTER "
            "(feature1__avg_over_1_week_rolling_windows FloatType);",
        ]
        query = cassandra_migration.create_query(diff, "table_name")

        assert query, expected_query

    def test_create_table_query(self):

        cassandra_migration = CassandraMigration()
        diff = {
            Diff("id", kind=Diff.Kind.CREATE, value=("FloatType", True)),
            Diff("new_feature", kind=Diff.Kind.CREATE, value=("FloatType", False)),
            Diff("test", kind=Diff.Kind.CREATE, value=("DoubleType", False)),
        }
        expected_query = [
            "CREATE TABLE test.table_name "
            "(new_feature FloatType, id FloatType, test DoubleType, PRIMARY KEY (id));"
        ]
        query = cassandra_migration.create_query(diff, "table_name")

        assert query, expected_query
