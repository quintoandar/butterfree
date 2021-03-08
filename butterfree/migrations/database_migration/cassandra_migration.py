"""Cassandra Migration entity."""

from typing import Any, Dict, List

from butterfree.clients import CassandraClient
from butterfree.configs.db import CassandraConfig
from butterfree.migrations.database_migration.database_migration import (
    DatabaseMigration,
)


class CassandraMigration(DatabaseMigration):
    """Cassandra class for Migrations."""

    def __init__(self) -> None:
        self._db_config = CassandraConfig()
        self._client = CassandraClient(
            host=[self._db_config.host],
            keyspace=self._db_config.keyspace,  # type: ignore
            user=self._db_config.username,
            password=self._db_config.password,
        )

    def create_query(
        self,
        table_name: str,
        db_schema: List[Dict[str, Any]] = None,
        diff_schema: List[Dict[str, Any]] = None,
    ) -> Any:
        """Create a query regarding Cassandra.

        Returns:
            Schema object.

        """
        pass
