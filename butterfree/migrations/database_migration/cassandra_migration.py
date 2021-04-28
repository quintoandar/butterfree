"""Cassandra Migration entity."""

from typing import Any, Dict, List

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
        - If an existing feature has its type changed, then it's extremely important to
        make sure that this conversion would not result in data loss;
        - If new features are added to your feature set, then they're going to be added
        to the corresponding Cassandra table;
        - Since feature sets can be written both to a feature set and an entity table,
        we're not going to automatically drop features when using entity tables, since
        it means that some features belong to a different feature set. In summary, if
        data is being loaded into an entity table, then users can drop columns manually.

    """

    def __init__(self) -> None:
        self._db_config = CassandraConfig()
        super(CassandraMigration, self).__init__(
            CassandraClient(
                host=[self._db_config.host],
                keyspace=self._db_config.keyspace,  # type: ignore
                user=self._db_config.username,
                password=self._db_config.password,
            )
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
        """Creates CQL statement to add columns to a table.

        Args:
            columns: list of Diff objects with ADD kind.
            table_name: table name.

        Returns:
            Alter table query.

        """
        parsed_columns = self._get_parsed_columns(columns)

        return f"ALTER TABLE {table_name} ADD ({parsed_columns});"

    def _get_alter_column_type_query(self, column: Diff, table_name: str) -> str:
        """Creates CQL statement to alter columns' types.

        Args:
            columns: list of Diff objects with ALTER_TYPE kind.
            table_name: table name.

        Returns:
            Alter column type query.

        """
        parsed_columns = self._get_parsed_columns([column])

        return (
            f"ALTER TABLE {table_name} ALTER {parsed_columns.replace(' ', ' TYPE ')};"
        )

    @staticmethod
    def _get_create_table_query(columns: List[Dict[str, Any]], table_name: str) -> str:
        """Creates CQL statement to create a table.

        Args:
            columns: object that contains column's schemas.
            table_name: table name.

        Returns:
            Create table query.

        """
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

    def _get_alter_table_drop_query(self, columns: List[Diff], table_name: str) -> str:
        """Creates CQL statement to drop columns from a table.

        Args:
            columns: list of Diff objects with DROP kind.
            table_name: table name.

        Returns:
            Drop columns from a given table query.

        """
        parsed_columns = self._get_parsed_columns(columns)

        return f"ALTER TABLE {table_name} DROP ({parsed_columns});"
