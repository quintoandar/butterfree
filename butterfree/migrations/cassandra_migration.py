"""Cassandra Migration entity."""

import warnings
from typing import Any, Dict, List

from butterfree.configs.db import CassandraConfig
from butterfree.migrations.migration import DatabaseMigration


class CassandraMigration(DatabaseMigration):
    """Cassandra class for Migrations."""

    @staticmethod
    def _get_alter_table_query(columns: List[Dict[str, Any]], table_name: str) -> str:
        parsed_columns = []
        for col in columns:
            parsed_columns.append(f"{col['column_name']} {col['type']}")

        parsed_columns = ", ".join(parsed_columns)  # type: ignore

        return f"ALTER TABLE {table_name} " f"ADD ({parsed_columns});"

    @staticmethod
    def _get_create_table_query(columns: List[Dict[str, Any]], table_name: str,) -> str:
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

        keyspace = CassandraConfig().keyspace

        return f"CREATE TABLE {keyspace}.{table_name} " f"({columns_str});"

    def create_query(
        self,
        table_name: str,
        db_schema: List[Dict[str, Any]] = None,
        schema_diff: List[Dict[str, Any]] = None,
    ) -> Any:
        """Create a query regarding Cassandra.

        Returns:
            Schema object.

        """
        if not schema_diff:
            warnings.warn("No migration was performed", UserWarning, stacklevel=1)
            return

        if not db_schema:
            return self._get_create_table_query(schema_diff, table_name)

        return self._get_alter_table_query(schema_diff, table_name)
