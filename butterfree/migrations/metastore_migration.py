"""Metastore Migration entity."""

from typing import Any

from butterfree.migrations import Migration


class MetastoreMigration(Migration):
    """Metastore class for Migrations."""

    def create_query(self) -> Any:
        """Create a query regarding Metastore.

        Returns:
            Schema object.

        """
        pass

    def apply_migration(self) -> None:
        """Apply the migration in Spark Metastore."""
        pass
