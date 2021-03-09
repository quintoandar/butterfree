"""Metastore Migration entity."""

from typing import Any, Set

from butterfree.clients import SparkClient
from butterfree.configs.db import MetastoreConfig
from butterfree.migrations.database_migration.database_migration import (
    DatabaseMigration,
    Diff,
)


class MetastoreMigration(DatabaseMigration):
    """Metastore class for Migrations."""

    def __init__(self) -> None:
        self._db_config = MetastoreConfig()
        self._client = SparkClient()

    def create_query(self, schema_diff: Set[Diff], table_name: str) -> Any:
        """Create a query regarding Metastore.

        Returns:
            Schema object.

        """
        pass
