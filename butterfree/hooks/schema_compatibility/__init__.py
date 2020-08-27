"""Holds Schema Compatibility Hooks definitions."""
from butterfree.hooks.schema_compatibility.cassandra_table_schema_compatibility_hook import (  # noqa
    CassandraTableSchemaCompatibilityHook,
)
from butterfree.hooks.schema_compatibility.spark_table_schema_compatibility_hook import (  # noqa
    SparkTableSchemaCompatibilityHook,
)

__all__ = ["SparkTableSchemaCompatibilityHook", "CassandraTableSchemaCompatibilityHook"]
