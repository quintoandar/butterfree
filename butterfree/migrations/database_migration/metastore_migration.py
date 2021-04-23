"""Metastore Migration entity."""

from typing import Any, Dict, List

from butterfree.clients import SparkClient
from butterfree.configs import environment
from butterfree.configs.db import MetastoreConfig
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

    def __init__(self, database: str = None,) -> None:
        self._db_config = MetastoreConfig()
        self.database = database or environment.get_variable(
            "FEATURE_STORE_HISTORICAL_DATABASE"
        )
        super(MetastoreMigration, self).__init__(SparkClient())

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

    def _get_alter_column_type_query(self, column: Diff, table_name: str) -> str:
        """Creates SQL statement to alter columns' types.

        Args:
            columns: list of Diff objects with ALTER_TYPE kind.
            table_name: table name.

        Returns:
            Alter column type query.

        """
        parsed_columns = self._get_parsed_columns([column])

        return f"ALTER TABLE {table_name} ALTER COLUMN {parsed_columns};"

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
        parsed_columns = []
        for col in columns:
            parsed_columns.append(f"{col['column_name']} {col['type']}")
        parsed_columns = ", ".join(parsed_columns)  # type: ignore

        return (
            f"CREATE TABLE IF NOT EXISTS  "
            f"{self.database}.{table_name} ({parsed_columns}) "
            f"PARTITIONED BY ("
            f"{PARTITION_BY[0]['column_name']} {PARTITION_BY[0]['type']}, "
            f"{PARTITION_BY[1]['column_name']} {PARTITION_BY[1]['type']}, "
            f"{PARTITION_BY[2]['column_name']} {PARTITION_BY[2]['type']});"
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
