"""Holds available database migrations."""

from butterfree.migrations.database_migration.cassandra_migration import (
    CassandraMigration,
)
from butterfree.migrations.database_migration.database_migration import Diff
from butterfree.migrations.database_migration.metastore_migration import (
    MetastoreMigration,
)

__all__ = ["CassandraMigration", "MetastoreMigration", "Diff"]


ALLOWED_DATABASE = {
    "cassandra": CassandraMigration(),
    "metastore": MetastoreMigration(),
}
