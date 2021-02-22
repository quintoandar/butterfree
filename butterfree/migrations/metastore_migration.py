"""Metastore Migration entity."""

import warnings
from typing import Any, Dict, List

from butterfree.configs import environment
from butterfree.constants.migrations import PARTITION_BY
from butterfree.migrations.migration import DatabaseMigration


class MetastoreMigration(DatabaseMigration):
    """Metastore class for Migrations."""

    def __init__(
        self, database: str = None,
    ):
        self.database = database or environment.get_variable(
            "FEATURE_STORE_HISTORICAL_DATABASE"
        )

    @staticmethod
    def _get_parsed_columns(table_columns: List[Dict[str, Any]]) -> str:
        parsed_columns = []
        for col in table_columns:
            parsed_columns.append(f"{col['column_name']} {col['type']}")

        parsed_columns = ", ".join(parsed_columns)  # type: ignore

        return parsed_columns

    def _get_alter_table_query(
        self, table_columns: List[Dict[str, Any]], table_name: str
    ) -> str:
        parsed_columns = self._get_parsed_columns(table_columns)

        return (
            f"ALTER TABLE {self.database}.{table_name} ADD columns ({parsed_columns});"
        )

    def _get_create_table_query(
        self, table_columns: List[Dict[str, Any]], table_name: str
    ) -> str:
        """Creates SQL statement to create a table."""
        table_columns.extend(PARTITION_BY)
        parsed_columns = self._get_parsed_columns(table_columns)

        return (
            f"CREATE TABLE {self.database}.{table_name} ({parsed_columns}) "
            f"PARTITIONED BY ({PARTITION_BY[0]['column_name']}, "
            f"{PARTITION_BY[1]['column_name']}, "
            f"{PARTITION_BY[2]['column_name']});"
        )

    def create_query(
        self,
        table_name: str,
        db_schema: List[Dict[str, Any]] = None,
        schema_diff: List[Dict[str, Any]] = None,
    ) -> Any:
        """Create a query regarding Metastore.
        Returns:
            Schema object.
        """
        if not schema_diff:
            warnings.warn("No migration was performed", UserWarning, stacklevel=1)
            return

        if not db_schema:
            return self._get_create_table_query(schema_diff, table_name)

        return self._get_alter_table_query(schema_diff, table_name)
