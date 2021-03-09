"""Cassandra Migration entity."""

import logging
from typing import Any, List, Set

from butterfree.clients import CassandraClient
from butterfree.configs.db import CassandraConfig
from butterfree.migrations.database_migration.database_migration import (
    DatabaseMigration,
    Diff,
)


class CassandraMigration(DatabaseMigration):
    """Cassandra class for performing migrations.

    This class implements some methods of the parent DatabaseMigration class and
    has specific methods for query building.

    The CassandraMigration class will be used, as the name suggests, for applying
    changes to a given Cassandra table. There are, however, some remarks that need
    to be highlighted:
        - If an existing feature has its type changed, then it'll be dropped and a new
        column with the same name and the new type will be created, therefore a
        backfilling job may be required;
        - If new features are added to your feature set, then they're going to be added
        to the corresponding Cassandra table;
        - Since feature sets can be written both to a feature set and an entity table,
        we're not going to automatically drop features, because, when using entity
        tables there are features that belongs to different feature sets.

    """

    def __init__(self) -> None:
        self._db_config = CassandraConfig()
        self._client = CassandraClient(
            host=[self._db_config.host],
            keyspace=self._db_config.keyspace,  # type: ignore
            user=self._db_config.username,
            password=self._db_config.password,
        )

    @staticmethod
    def _get_alter_table_add_query(columns: List[Diff], table_name: str) -> str:
        parsed_columns = []
        for col in columns:
            parsed_columns.append(f"{col.column} {col.value}")

        parsed_columns = ", ".join(parsed_columns)  # type: ignore

        return f"ALTER TABLE {table_name} ADD ({parsed_columns});"

    @staticmethod
    def _get_alter_column_type_query(columns: List[Diff], table_name: str) -> str:
        parsed_columns = []
        for col in columns:
            parsed_columns.append(f"{col.column} {col.value}")

        parsed_columns = ", ".join(parsed_columns)  # type: ignore

        return f"ALTER TABLE {table_name} ALTER ({parsed_columns});"

    @staticmethod
    def _get_create_table_query(columns: List[Diff], table_name: str,) -> str:
        """Creates CQL statement to create a table."""
        parsed_columns = []
        primary_keys = []

        for col in columns:
            col_str = f"{col.column} {col.value[0]}"
            if col.value[1]:
                primary_keys.append(col.column)
            parsed_columns.append(col_str)

        joined_parsed_columns = ", ".join(parsed_columns)

        if len(primary_keys) > 0:
            joined_primary_keys = ", ".join(primary_keys)
            columns_str = (
                f"{joined_parsed_columns}, PRIMARY KEY ({joined_primary_keys})"
            )
        else:
            columns_str = joined_parsed_columns

        keyspace = CassandraConfig().keyspace

        return f"CREATE TABLE {keyspace}.{table_name} " f"({columns_str});"

    @staticmethod
    def _get_alter_table_drop_query(columns: List[Diff], table_name: str) -> str:
        parsed_columns = []
        for col in columns:
            parsed_columns.append(f"{col.column}")

        parsed_columns = ", ".join(parsed_columns)  # type: ignore

        return f"ALTER TABLE {table_name} DROP ({parsed_columns});"

    def create_query(self, schema_diff: Set[Diff], table_name: str) -> Any:
        """Create a query regarding Cassandra.

        Returns:
            Schema object.

        """
        queries = []
        create_items = [item for item in schema_diff if item.kind == Diff.Kind.CREATE]
        add_items = [item for item in schema_diff if item.kind == Diff.Kind.ADD]
        drop_items = [item for item in schema_diff if item.kind == Diff.Kind.DROP]
        alter_type_items = [
            item for item in schema_diff if item.kind == Diff.Kind.ALTER_TYPE
        ]
        alter_key_items = [
            item for item in schema_diff if item.kind == Diff.Kind.ALTER_KEY
        ]

        if create_items:
            create_table_query = self._get_create_table_query(create_items, table_name)
            queries.append(create_table_query)
        if add_items:
            alter_table_add_query = self._get_alter_table_add_query(
                add_items, table_name
            )
            queries.append(alter_table_add_query)
        if drop_items:
            drop_columns_query = self._get_alter_table_drop_query(
                drop_items, table_name
            )
            queries.append(drop_columns_query)
        if alter_type_items:
            alter_column_types_query = self._get_alter_column_type_query(
                alter_type_items, table_name
            )
            queries.append(alter_column_types_query)
        if alter_key_items:
            logging.info("This operations is not supported by Cassandra DB.")
        return queries
