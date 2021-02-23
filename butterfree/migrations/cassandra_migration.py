"""Cassandra Migration entity."""

from typing import Any, Dict, List

from butterfree.configs.db import CassandraConfig
from butterfree.migrations.migration import DatabaseMigration


class CassandraMigration(DatabaseMigration):
    """Cassandra class for performing migrations.

    This class implements some methods of the parent DatabaseMigration class and
    has specific methods for query building.

    The CassandraMigration class will be used, as the name suggests, for applying
    changes to a given Cassandra table. There are, however, some remarks that need
    to be highlighted:
        - If an existing feature is renamed, then it'll be dropped and a new column
        will be created, therefore a backfilling job may be required. The same logic
        applies to data type changes;
        - If new features are added to your feature set, then they're going to be added
        to the corresponding Cassandra table;
        - Since feature sets can be written both to a feature set and an entity table,
        we're not going to automatically drop features, because, when using entity
        tables there are features that belongs to different feature sets.

    """

    @staticmethod
    def _get_alter_table_add_query(
        columns: List[Dict[str, Any]], table_name: str
    ) -> str:
        parsed_columns = []
        for col in columns:
            parsed_columns.append(f"{col['column_name']} {col['type']}")

        parsed_columns = ", ".join(parsed_columns)  # type: ignore

        return f"ALTER TABLE {table_name} ADD ({parsed_columns});"

    @staticmethod
    def _get_create_table_query(columns: List[Dict[str, Any]], table_name: str,) -> str:
        """Creates CQL statement to create a table."""
        parsed_columns = []
        primary_keys = []

        for col in columns:
            col_str = f"{col['column_name']} {col['type']}"
            if col["primary_key"]:
                primary_keys.append(col["column_name"])
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
    def _get_alter_table_drop_query(
        columns: List[Dict[str, Any]], table_name: str
    ) -> str:
        parsed_columns = []
        for col in columns:
            parsed_columns.append(f"{col['column_name']}")

        parsed_columns = ", ".join(parsed_columns)  # type: ignore

        return f"ALTER TABLE {table_name} DROP ({parsed_columns});"

    def create_query(
        self,
        table_name: str,
        schema_diff: List[Dict[str, Any]],
        db_schema: List[Dict[str, Any]] = None,
        features_with_diff_types: List[Dict[str, Any]] = None,
    ) -> List[str]:
        """Create a query regarding Cassandra.

        Returns:
            Schema object.

        """
        if not db_schema:
            create_table_query = self._get_create_table_query(schema_diff, table_name)
            return [create_table_query]

        alter_table_query = self._get_alter_table_add_query(schema_diff, table_name)

        if features_with_diff_types:
            drop_table_query = self._get_alter_table_drop_query(
                features_with_diff_types, table_name
            )
            return [alter_table_query, drop_table_query]

        return [alter_table_query]
