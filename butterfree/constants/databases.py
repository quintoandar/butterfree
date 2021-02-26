"""Allowed databases for migration."""
from butterfree.migrations.database_migration import (
    CassandraMigration,
    MetastoreMigration,
)

ALLOWED_DATABASE = {
    "cassandra": CassandraMigration(),
    "metastore": MetastoreMigration(),
}
