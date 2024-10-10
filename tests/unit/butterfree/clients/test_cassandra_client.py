from typing import Any, Dict, List
from unittest.mock import MagicMock

import pytest

from butterfree.clients import CassandraClient
from butterfree.clients.cassandra_client import (
    EMPTY_STRING_HOST_ERROR,
    GENERIC_INVALID_HOST_ERROR,
    CassandraColumn,
)


def sanitize_string(query: str) -> str:
    """Remove multiple spaces and new lines"""
    return " ".join(query.split())


class TestCassandraClient:
    def test_conn(self, cassandra_client: CassandraClient) -> None:
        # arrange
        cassandra_client = CassandraClient(host=["mock"], keyspace="dummy_keyspace")

        # act
        start_conn = cassandra_client._session

        # assert
        assert start_conn is None

    def test_cassandra_client_sql(
        self,
        cassandra_client: CassandraClient,
        cassandra_feature_set: List[Dict[str, Any]],
    ) -> None:
        cassandra_client.sql = MagicMock(  # type: ignore
            return_value=cassandra_feature_set
        )

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

    def test_cassandra_get_schema(self, cassandra_client: CassandraClient) -> None:
        cassandra_client.sql = MagicMock(  # type: ignore
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

    def test_cassandra_create_table(
        self,
        cassandra_client: CassandraClient,
        cassandra_feature_set: List[Dict[str, Any]],
    ) -> None:
        cassandra_client.sql = MagicMock()  # type: ignore

        columns: List[CassandraColumn] = [
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

    def test_initialize_with_string_host(self):
        client = CassandraClient(
            host="192.168.1.1, 192.168.1.2", keyspace="dummy_keyspace"
        )
        assert client.host == ["192.168.1.1", "192.168.1.2"]

    def test_initialize_with_list_host(self):
        client = CassandraClient(
            host=["192.168.1.1", "192.168.1.2"], keyspace="test_keyspace"
        )
        assert client.host == ["192.168.1.1", "192.168.1.2"]

    def test_initialize_with_empty_string_host(self):
        with pytest.raises(
            ValueError,
            match=EMPTY_STRING_HOST_ERROR,
        ):
            CassandraClient(host="", keyspace="test_keyspace")

    def test_initialize_with_none_host(self):
        with pytest.raises(
            ValueError,
            match=GENERIC_INVALID_HOST_ERROR,
        ):
            CassandraClient(host=None, keyspace="test_keyspace")

    def test_initialize_with_invalid_host_type(self):
        with pytest.raises(
            ValueError,
            match=GENERIC_INVALID_HOST_ERROR,
        ):
            CassandraClient(host=123, keyspace="test_keyspace")

    def test_initialize_with_invalid_list_host(self):
        with pytest.raises(
            ValueError,
            match=GENERIC_INVALID_HOST_ERROR,
        ):
            CassandraClient(host=["192.168.1.1", 123], keyspace="test_keyspace")

    def test_initialize_with_list_of_string_hosts(self):
        client = CassandraClient(
            host=["192.168.1.1, 192.168.1.2"], keyspace="test_keyspace"
        )
        assert client.host == ["192.168.1.1", "192.168.1.2"]
