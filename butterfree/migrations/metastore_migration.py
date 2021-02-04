"""Metastore Migration entity."""

from typing import Any

from butterfree.migrations import Migration


class MetastoreMigration(Migration):
    """Metastore class for Migrations."""

    def run_query(self) -> Any:
        """Execute a query regarding Metastore.

        Returns:
            Schema object.

        """
        pass

    def apply_migration(self) -> None:
        """Apply the migration in Spark Metastore."""
        pass
