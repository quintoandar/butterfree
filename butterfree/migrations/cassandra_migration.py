"""Cassandra Migration entity."""

from typing import Any

from butterfree.migrations import Migration


class CassandraMigration(Migration):
    """Cassandra class for Migrations."""

    def create_query(self) -> Any:
        """Create a query regarding Cassandra.

        Returns:
            Schema object.

        """
        pass

    def apply_migration(self) -> None:
        """Apply the migration in Cassandra."""
        pass
