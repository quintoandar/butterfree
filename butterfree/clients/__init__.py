"""Holds connection clients."""

from butterfree.clients.abstract_client import AbstractClient
from butterfree.clients.spark_client import SparkClient

__all__ = ["SparkClient", "AbstractClient"]

try:
    from butterfree.clients.cassandra_client import CassandraClient  # noqa: F401

    __all__.append("CassandraClient")
except ImportError:
    pass
