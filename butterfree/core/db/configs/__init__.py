"""This module holds database configurations to be used by clients."""

from butterfree.core.db.configs.cassandra_config import CassandraConfig
from butterfree.core.db.configs.s3_config import S3Config

__all__ = ["CassandraConfig", "S3Config"]
