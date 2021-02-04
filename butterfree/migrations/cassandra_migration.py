"""Cassandra Migration entity."""

from typing import Any

from butterfree.migrations import Migration


class CassandraMigration(Migration):
    """Cassandra class for Migrations."""

    def run_query(self) -> Any:
        """Execute a query regarding Cassandra.

        Returns:
            Schema object.

        """
        pass

    def apply_migration(self) -> None:
        """Apply the migration in Cassandra."""
        pass
