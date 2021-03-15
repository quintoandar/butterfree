"""Metastore Migration entity."""

import logging
from typing import Any, Dict, List, Set

from butterfree.configs import environment
from butterfree.constants.migrations import PARTITION_BY
from butterfree.migrations.database_migration.database_migration import (
    DatabaseMigration,
    Diff,
)


class MetastoreMigration(DatabaseMigration):
    """MetastoreMigration class for performing migrations.

    This class implements some methods of the parent DatabaseMigration class and
    has specific methods for query building.
    The MetastoreMigration class will be used, as the name suggests, for applying
    changes to a given Metastore table. There are, however, some remarks that need
    to be highlighted:
        - If an existing feature has its type changed, then it's extremely important to
        make sure that this conversion would not result in data loss;
        - If new features are added to your feature set, then they're going to be added
        to the corresponding Metastore table;
        - Since feature sets can be written both to a feature set and an entity table,
        we're not going to automatically drop features when using entity tables, since
        it means that some features belong to a different feature set. In summary, if
        data is being loaded into an entity table, then users can drop columns manually.
    """

    def __init__(
        self, database: str = None,
    ):
        self.database = database or environment.get_variable(
            "FEATURE_STORE_HISTORICAL_DATABASE"
        )

    @staticmethod
    def _get_parsed_columns(columns: List[Diff]) -> List[str]:
        """Parse columns from a list of Diff objects.

        Args:
            columns: list of Diff objects.

        Returns:
            Parsed columns.

        """
        parsed_columns = []
        for col in columns:
            parsed_columns.append(f"{col.column} {col.value}")

        parsed_columns = ", ".join(parsed_columns)  # type: ignore

        return parsed_columns

    def _get_alter_table_add_query(self, columns: List[Diff], table_name: str) -> str:
        """Creates SQL statement to add columns to a table.

        Args:
            columns: list of Diff objects with ADD kind.
            table_name: table name.

        Returns:
            Alter table query.

        """
        parsed_columns = self._get_parsed_columns(columns)

        return (
            f"ALTER TABLE {self.database}.{table_name} "
            f"ADD IF NOT EXISTS columns ({parsed_columns});"
        )

    def _get_alter_column_type_query(self, columns: List[Diff], table_name: str) -> str:
        """Creates SQL statement to alter columns' types.

        Args:
            columns: list of Diff objects with ALTER_TYPE kind.
            table_name: table name.

        Returns:
            Alter column type query.

        """
        parsed_columns = self._get_parsed_columns(columns)

        return f"ALTER TABLE {table_name} ALTER COLUMN ({parsed_columns});"

    def _get_create_table_query(
        self, columns: List[Dict[str, Any]], table_name: str
    ) -> str:
        """Creates SQL statement to create a table.

        Args:
            columns: object that contains column's schemas.
            table_name: table name.

        Returns:
            Create table query.

        """
        columns.extend(PARTITION_BY)

        parsed_columns = []
        for col in columns:
            parsed_columns.append(f"{col['column_name']} {col['type']}")
        parsed_columns = ", ".join(parsed_columns)  # type: ignore

        return (
            f"CREATE TABLE IF NOT EXISTS  "
            f"{self.database}.{table_name} ({parsed_columns}) "
            f"PARTITIONED BY ({PARTITION_BY[0]['column_name']}, "
            f"{PARTITION_BY[1]['column_name']}, "
            f"{PARTITION_BY[2]['column_name']});"
        )

    def _get_alter_table_drop_query(self, columns: List[Diff], table_name: str) -> str:
        """Creates SQL statement to drop columns from a table.

        Args:
            columns: list of Diff objects with DROP kind.
            table_name: table name.

        Returns:
            Drop columns from a given table query.

        """
        parsed_columns = self._get_parsed_columns(columns)

        return f"ALTER TABLE {table_name} DROP IF EXISTS ({parsed_columns});"

    def _get_queries(
        self, schema_diff: Set[Diff], table_name: str, write_on_entity: bool = None
    ) -> List[str]:
        """Create the desired queries for migration.

        Args:
            schema_diff: list of Diff objects.
            table_name: table name.

        Returns:
            List of queries.
        """
        add_items = []
        drop_items = []
        alter_type_items = []
        alter_key_items = []

        for diff in schema_diff:
            if diff.kind == Diff.Kind.ADD:
                add_items.append(diff)
            elif diff.kind == Diff.Kind.ALTER_TYPE:
                alter_type_items.append(diff)
            elif diff.kind == Diff.Kind.DROP:
                drop_items.append(diff)
            elif diff.kind == Diff.Kind.ALTER_KEY:
                alter_key_items.append(diff)

        queries = []
        if add_items:
            alter_table_add_query = self._get_alter_table_add_query(
                add_items, table_name
            )
            queries.append(alter_table_add_query)
        if drop_items:
            if write_on_entity:
                logging.info(
                    "Features will not be dropped automatically "
                    "when data is loaded to an entity table"
                )
            else:
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
            logging.info("This operation is not supported by Spark.")

        return queries
