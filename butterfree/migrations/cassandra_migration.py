"""Cassandra Migration entity."""

from typing import Any, Dict, List

from butterfree.migrations.migration import DatabaseMigration


class CassandraMigration(DatabaseMigration):
    """Cassandra class for Migrations."""

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
        pass
