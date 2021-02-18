"""Cassandra Migration entity."""

from typing import Any, Dict, List

from butterfree.migrations import DatabaseMigration


class CassandraMigration(DatabaseMigration):
    """Cassandra class for Migrations."""

    def create_query(
        self,
        fs_schema: List[Dict[str, Any]],
        db_schema: List[Dict[str, Any]],
        table_name: str,
    ) -> Any:
        """Create a query regarding Cassandra.

        Returns:
            Schema object.

        """
        pass
