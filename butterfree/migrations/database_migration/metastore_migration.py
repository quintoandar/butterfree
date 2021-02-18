"""Metastore Migration entity."""

from typing import Any, Dict, List

from butterfree.migrations import DatabaseMigration


class MetastoreMigration(DatabaseMigration):
    """Metastore class for Migrations."""

    def create_query(
        self,
        fs_schema: List[Dict[str, Any]],
        db_schema: List[Dict[str, Any]],
        table_name: str,
    ) -> Any:
        """Create a query regarding Metastore.

        Returns:
            Schema object.

        """
        pass
