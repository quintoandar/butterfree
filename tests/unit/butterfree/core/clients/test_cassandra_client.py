from unittest.mock import MagicMock

import pytest

from butterfree.core.clients import CassandraClient


def sanitize_string(query):
    """Remove multiple spaces and new lines"""
    return " ".join(query.split())


class TestCassandraClient:
    def test_conn(self):
        # arrange
        cassandra_client = CassandraClient(
            cassandra_host=["mock"], cassandra_key_space="dummy_keyspace"
        )

        # act
        start_conn = cassandra_client._session
        get_conn1 = cassandra_client.conn
        get_conn2 = cassandra_client.conn

        # assert
        assert start_conn is None
        assert get_conn1 == get_conn2

    def test_cassandra_get_table_schema(self, cassandra_client):
        cassandra_client._session = MagicMock()
        cassandra_client._session.execute = MagicMock(
            return_value=[
                {"column_name": "feature1", "type": "text"},
                {"column_name": "feature2", "type": "bigint"},
            ]
        )

        table = "dummy_table"

        expected_query = """
                SELECT column_name, type FROM system_schema.columns
                WHERE keyspace_name = 'dummy_keyspace'
                AND table_name = 'dummy_table';
                """

        cassandra_client.get_table_schema(table)
        query = cassandra_client._session.execute.call_args[0][0]

        assert sanitize_string(query) == sanitize_string(expected_query)

    def test_cassandra_create_table(self, cassandra_client, cassandra_feature_set):
        cassandra_client._session = MagicMock()

        columns = [
            {"column_name": "id", "type": "int", "primary_key": True},
            {"column_name": "rent_per_month", "type": "float", "primary_key": False},
        ]
        table = "dummy_table"

        expected_query = """
            CREATE TABLE dummy_keyspace.dummy_table
            (id int, rent_per_month float, PRIMARY KEY (id))
            WITH
                bloom_filter_fp_chance = 0.01
                AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
                AND comment = ''
                AND compaction =
                {'class':
                'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy',
                'max_threshold': '32', 'min_threshold': '4'}
                AND compression = {'chunk_length_in_kb': '64',
                'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
                AND crc_check_chance = 1.0
                AND dclocal_read_repair_chance = 0.1
                AND default_time_to_live = 0
                AND gc_grace_seconds = 864000
                AND max_index_interval = 2048
                AND memtable_flush_period_in_ms = 0
                AND min_index_interval = 128
                AND read_repair_chance = 0.0
                AND speculative_retry = '99PERCENTILE';
                """

        cassandra_client.create_table(columns, table)
        query = cassandra_client._session.execute.call_args[0][0]

        assert sanitize_string(query) == sanitize_string(expected_query)

    def test_cassandra_create_table_custom_arguments(self, cassandra_client):
        cassandra_client._session = MagicMock()

        columns = [
            {"column_name": "id", "type": "int", "primary_key": True},
            {"column_name": "rent_per_month", "type": "float", "primary_key": False},
        ]
        table = "dummy_table"

        bloom_filter_fp_chance = 0.02
        caching = {"test": "cool"}
        comment = "awesome test"
        compaction = {"kappa": "pride"}
        compression = {"test": "again"}
        crc_check_chance = 0.17
        dclocal_read_repair_chance = 0.02
        default_time_to_live = 12
        gc_grace_seconds = 3
        max_index_interval = 27
        memtable_flush_period_in_ms = 2
        min_index_interval = 17
        read_repair_chance = 0.3
        speculative_retry = "what?"

        expected_query = """
            CREATE TABLE dummy_keyspace.dummy_table
            (id int, rent_per_month float, PRIMARY KEY (id))
            WITH
                bloom_filter_fp_chance = 0.02
                AND caching = {'test': 'cool'}
                AND comment = 'awesome test'
                AND compaction = {'kappa': 'pride'}
                AND compression = {'test': 'again'}
                AND crc_check_chance = 0.17
                AND dclocal_read_repair_chance = 0.02
                AND default_time_to_live = 12
                AND gc_grace_seconds = 3
                AND max_index_interval = 27
                AND memtable_flush_period_in_ms = 2
                AND min_index_interval = 17
                AND read_repair_chance = 0.3
                AND speculative_retry = 'what?';
                """

        cassandra_client.create_table(
            columns,
            table,
            bloom_filter_fp_chance=bloom_filter_fp_chance,
            caching=caching,
            comment=comment,
            compaction=compaction,
            compression=compression,
            crc_check_chance=crc_check_chance,
            dclocal_read_repair_chance=dclocal_read_repair_chance,
            default_time_to_live=default_time_to_live,
            gc_grace_seconds=gc_grace_seconds,
            max_index_interval=max_index_interval,
            memtable_flush_period_in_ms=memtable_flush_period_in_ms,
            min_index_interval=min_index_interval,
            read_repair_chance=read_repair_chance,
            speculative_retry=speculative_retry,
        )
        query = cassandra_client._session.execute.call_args[0][0]

        assert sanitize_string(query) == sanitize_string(expected_query)

    def test_cassandra_execute_query(self, cassandra_client, cassandra_feature_set):
        cassandra_client.execute_query = MagicMock(return_value=cassandra_feature_set)

        assert isinstance(
            cassandra_client.execute_query(
                "select feature1, feature2 from cassandra_feature_set"
            ),
            list,
        )
        assert all(
            isinstance(elem, dict)
            for elem in cassandra_client.execute_query(
                "select feature1, feature2 from cassandra_feature_set"
            )
        )

    def test_cassandra_without_session(self, cassandra_client):
        cassandra_client = cassandra_client

        with pytest.raises(
            RuntimeError, match="There's no session available for this query."
        ):
            cassandra_client.execute_query(
                query="select feature1, feature2 from cassandra_feature_set"
            )
        with pytest.raises(
            RuntimeError, match="There's no session available for this query."
        ):
            cassandra_client.create_table(["feature1", "feature2"], "test")
        with pytest.raises(
            RuntimeError, match="There's no session available for this query."
        ):
            cassandra_client.get_table_schema("test")
