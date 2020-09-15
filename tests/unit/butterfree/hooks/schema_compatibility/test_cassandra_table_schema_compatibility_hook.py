from unittest.mock import MagicMock

import pytest

from butterfree.clients import CassandraClient
from butterfree.hooks.schema_compatibility import CassandraTableSchemaCompatibilityHook


class TestCassandraTableSchemaCompatibilityHook:
    def test_run_compatible_schema(self, spark_session):
        cassandra_client = CassandraClient(
            cassandra_host=["mock"], cassandra_keyspace="dummy_keyspace"
        )

        cassandra_client.sql = MagicMock(  # type: ignore
            return_value=[
                {"column_name": "feature1", "type": "text"},
                {"column_name": "feature2", "type": "int"},
            ]
        )

        table = "table"

        input_dataframe = spark_session.sql("select 'abc' as feature1, 1 as feature2")

        hook = CassandraTableSchemaCompatibilityHook(cassandra_client, table)

        # act and assert
        assert hook.run(input_dataframe) == input_dataframe

    def test_run_incompatible_schema(self, spark_session):
        cassandra_client = CassandraClient(
            cassandra_host=["mock"], cassandra_keyspace="dummy_keyspace"
        )

        cassandra_client.sql = MagicMock(  # type: ignore
            return_value=[
                {"column_name": "feature1", "type": "text"},
                {"column_name": "feature2", "type": "bigint"},
            ]
        )

        table = "table"

        input_dataframe = spark_session.sql("select 'abc' as feature1, 1 as feature2")

        hook = CassandraTableSchemaCompatibilityHook(cassandra_client, table)

        # act and assert
        with pytest.raises(
            ValueError, match="There's a schema incompatibility between"
        ):
            hook.run(input_dataframe)
