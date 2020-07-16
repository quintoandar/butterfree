from unittest.mock import MagicMock

import pytest

from butterfree.clients import CassandraClient


def sanitize_string(query):
    """Remove multiple spaces and new lines"""
    return " ".join(query.split())


class TestCassandraClient:
    def test_conn(self, cassandra_client):
        # arrange
        cassandra_client = CassandraClient(
            cassandra_host=["mock"], cassandra_key_space="dummy_keyspace"
        )

        # act
        start_conn = cassandra_client._session

        # assert
        assert start_conn is None

    def test_cassandra_client_sql(self, cassandra_client, cassandra_feature_set):
        cassandra_client.sql = MagicMock(return_value=cassandra_feature_set)

        assert isinstance(
            cassandra_client.sql(
                "select feature1, feature2 from cassandra_feature_set"
            ),
            list,
        )
        assert all(
            isinstance(elem, dict)
            for elem in cassandra_client.sql(
                "select feature1, feature2 from cassandra_feature_set"
            )
        )

    def test_cassandra_get_schema(self, cassandra_client):
        cassandra_client.sql = MagicMock(
            return_value=[
                {"column_name": "feature1", "type": "text"},
                {"column_name": "feature2", "type": "bigint"},
            ]
        )

        table = "table"

        expected_query = (
            f"SELECT column_name, type FROM system_schema.columns "  # noqa
            f"WHERE keyspace_name = 'dummy_keyspace' "  # noqa
            f"AND table_name = '{table}';"  # noqa
        )

        cassandra_client.get_schema(table)
        query = cassandra_client.sql.call_args[0][0]

        assert sanitize_string(query) == sanitize_string(expected_query)

    def test_cassandra_create_table(self, cassandra_client, cassandra_feature_set):
        cassandra_client.sql = MagicMock()

        columns = [
            {"column_name": "id", "type": "int", "primary_key": True},
            {"column_name": "rent_per_month", "type": "float", "primary_key": False},
        ]
        table = "dummy_table"

        expected_query = """
            CREATE TABLE dummy_keyspace.dummy_table
            (id int, rent_per_month float, PRIMARY KEY (id));
                """

        cassandra_client.create_table(columns, table)
        query = cassandra_client.sql.call_args[0][0]

        assert sanitize_string(query) == sanitize_string(expected_query)

    def test_cassandra_without_session(self, cassandra_client):
        cassandra_client = cassandra_client

        with pytest.raises(
            RuntimeError, match="There's no session available for this query."
        ):
            cassandra_client.sql(
                query="select feature1, feature2 from cassandra_feature_set"
            )
        with pytest.raises(
            RuntimeError, match="There's no session available for this query."
        ):
            cassandra_client.create_table(
                [
                    {"column_name": "id", "type": "int", "primary_key": True},
                    {
                        "column_name": "rent_per_month",
                        "type": "float",
                        "primary_key": False,
                    },
                ],
                "test",
            )
        with pytest.raises(
            RuntimeError, match="There's no session available for this query."
        ):
            cassandra_client.get_schema("test")
