"""Metastore Migration entity."""

from typing import Any, Dict, List

from butterfree.clients import SparkClient
from butterfree.configs.db import MetastoreConfig
from butterfree.migrations.database_migration.database_migration import (
    DatabaseMigration,
)


class MetastoreMigration(DatabaseMigration):
    """Metastore class for Migrations."""

    def __init__(self) -> None:
        self._db_config = MetastoreConfig()
        self._client = SparkClient()

    def create_query(
        self,
        fs_schema: List[Dict[str, Any]],
        table_name: str,
        db_schema: List[Dict[str, Any]] = None,
        write_on_entity: bool = None,
    ) -> Any:
        """Create a query regarding Metastore.

        Returns:
            Schema object.

        """
        pass
