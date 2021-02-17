"""Cassandra Migration entity."""

import logging
from typing import Any

from butterfree.clients import CassandraClient
from butterfree.migrations import Migration


class CassandraMigration(Migration):
    """Cassandra class for Migrations."""

    CASSANDRA_KEYSPACE = "abc"

    @staticmethod
    def _get_alter_table_query(columns, table_name):
        parsed_columns = []
        for col in columns:
            parsed_columns.append(f"{col['column_name']} {col['type']}")

        parsed_columns = ", ".join(parsed_columns)

        return f"ALTER TABLE {table_name} " f"ADD ({parsed_columns});"

    def _get_create_table_query(
        self, columns, table_name,
    ):
        """Creates CQL statement to create a table."""
        parsed_columns = []
        primary_keys = []

        for col in columns:
            col_str = f"{col['column_name']} {col['type']}"
            if col["primary_key"]:
                primary_keys.append(col["column_name"])
            parsed_columns.append(col_str)

        joined_parsed_columns = ", ".join(parsed_columns)

        if len(primary_keys) > 0:
            joined_primary_keys = ", ".join(primary_keys)
            columns_str = (
                f"{joined_parsed_columns}, PRIMARY KEY ({joined_primary_keys})"
            )
        else:
            columns_str = joined_parsed_columns

        return (
            f"CREATE TABLE {self.CASSANDRA_KEYSPACE}.{table_name} " f"({columns_str}); "
        )

    def create_query(
        self, fs_schema, db_schema, table_name,
    ):
        features_to_add = [
            {"column_name": x.name, "type": x.type}
            for x in fs_schema
            if x.name not in db_schema.keys()
        ]

        if not features_to_add:
            return

        if not db_schema:
            return self._get_create_table_query(features_to_add, table_name)

        return self._get_alter_table_query(features_to_add, table_name)

    def apply_migration(self, query, client: CassandraClient,) -> Any:
        """Apply the migration in Cassandra."""
        client.sql(query)
