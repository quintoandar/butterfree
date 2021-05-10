"""Migration entity."""
import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass
from enum import Enum, auto
from typing import Any, Dict, List, Set

from butterfree.clients import AbstractClient
from butterfree.configs.logger import __logger
from butterfree.load.writers.writer import Writer
from butterfree.transform import FeatureSet

logger = __logger("database_migrate", True)


@dataclass
class Diff:
    """DataClass to help identifying different types of diff between schemas."""

    class Kind(Enum):
        """Mapping actions to take given a difference between columns of a schema."""

        ADD = auto()
        ALTER_KEY = auto()
        ALTER_TYPE = auto()
        DROP = auto()

    column: str
    kind: Kind
    value: Any

    def __hash__(self) -> int:
        return hash((self.column, self.kind, self.value))

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, type(self)):
            raise NotImplementedError
        return (
            self.column == other.column
            and self.kind == other.kind
            and self.value == other.value
        )


class DatabaseMigration(ABC):
    """Abstract base class for Migrations."""

    def __init__(self, client: AbstractClient) -> None:
        self._client = client

    @abstractmethod
    def _get_create_table_query(
        self, columns: List[Dict[str, Any]], table_name: str
    ) -> Any:
        """Creates desired statement to create a table.

        Args:
            columns: object that contains column's schemas.
            table_name: table name.

        Returns:
            Create table query.

        """
        pass

    @abstractmethod
    def _get_alter_table_add_query(self, columns: List[Diff], table_name: str) -> str:
        """Creates desired statement to add columns to a table.

        Args:
            columns: list of Diff objects with ADD kind.
            table_name: table name.

        Returns:
            Alter table query.

        """
        pass

    @abstractmethod
    def _get_alter_table_drop_query(self, columns: List[Diff], table_name: str) -> str:
        """Creates desired statement to drop columns from a table.

        Args:
            columns: list of Diff objects with DROP kind.
            table_name: table name.

        Returns:
            Drop columns from a given table query.

        """
        pass

    @abstractmethod
    def _get_alter_column_type_query(self, column: Diff, table_name: str) -> str:
        """Creates desired statement to alter columns' types.

        Args:
            columns: list of Diff objects with ALTER_TYPE kind.
            table_name: table name.

        Returns:
            Alter column type query.

        """
        pass

    def _get_queries(
        self, schema_diff: Set[Diff], table_name: str, write_on_entity: bool = None
    ) -> Any:
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
            for item in alter_type_items:
                alter_column_types_query = self._get_alter_column_type_query(
                    item, table_name
                )
                queries.append(alter_column_types_query)
        if alter_key_items:
            logger.info("This operation is not supported by Spark.")

        return queries

    def create_query(
        self,
        fs_schema: List[Dict[str, Any]],
        table_name: str,
        db_schema: List[Dict[str, Any]] = None,
        write_on_entity: bool = None,
    ) -> Any:
        """Create a query regarding a data source.

        Returns:
            The desired queries for the given database.

        """
        if not db_schema:
            return [self._get_create_table_query(fs_schema, table_name)]

        schema_diff = self._get_diff(fs_schema, db_schema)

        return self._get_queries(schema_diff, table_name, write_on_entity)

    @staticmethod
    def _get_diff(
        fs_schema: List[Dict[str, Any]], db_schema: List[Dict[str, Any]],
    ) -> Set[Diff]:
        """Gets schema difference between feature set and the table of a given db.

        Args:
            fs_schema: object that contains feature set's schemas.
            db_schema: object that contains the table of a given db schema.

        Returns:
            Object with schema differences.

        """
        db_columns = set(item.get("column_name") for item in db_schema)
        fs_columns = set(item.get("column_name") for item in fs_schema)

        add_columns = fs_columns - db_columns
        drop_columns = db_columns - fs_columns

        # This could be way easier to write (and to read) if the schemas were a simple
        # Dict[str, Any] where each key would be the column name itself...
        # but changing that could break things so:
        # TODO version 2 change get schema to return a dict(columns, properties)
        add_type_columns = dict()
        alter_type_columns = dict()
        alter_key_columns = dict()
        for fs_item in fs_schema:
            if fs_item.get("column_name") in add_columns:
                add_type_columns.update(
                    {fs_item.get("column_name"): fs_item.get("type")}
                )
            for db_item in db_schema:
                if fs_item.get("column_name") == db_item.get("column_name"):
                    if fs_item.get("type") != db_item.get("type"):
                        alter_type_columns.update(
                            {fs_item.get("column_name"): fs_item.get("type")}
                        )
                    if fs_item.get("primary_key") != db_item.get("primary_key"):
                        alter_key_columns.update(
                            {fs_item.get("column_name"): fs_item.get("primary_key")}
                        )
                    break

        schema_diff = set(
            Diff(str(col), kind=Diff.Kind.ADD, value=value)
            for col, value in add_type_columns.items()
        )
        schema_diff |= set(
            Diff(str(col), kind=Diff.Kind.DROP, value=None) for col in drop_columns
        )
        schema_diff |= set(
            Diff(str(col), kind=Diff.Kind.ALTER_TYPE, value=value)
            for col, value in alter_type_columns.items()
        )
        schema_diff |= set(
            Diff(str(col), kind=Diff.Kind.ALTER_KEY, value=None)
            for col, value in alter_key_columns.items()
        )
        return schema_diff

    def _get_schema(
        self, table_name: str, database: str = None
    ) -> List[Dict[str, Any]]:
        """Get a table schema in the respective database.

        Args:
            table_name: Table name to get schema.

        Returns:
            Schema object.
        """
        try:
            db_schema = self._client.get_schema(table_name, database)
        except Exception:  # noqa
            db_schema = []
        return db_schema

    def apply_migration(
        self, feature_set: FeatureSet, writer: Writer, debug_mode: bool
    ) -> None:
        """Apply the migration in the respective database.

        Args:
            feature_set: the feature set.
            writer: the writer being used to load the feature set.
            debug_mode: if active, it brings up the queries generated.
        """
        logger.info(f"Migrating feature set: {feature_set.name}")

        table_name = (
            feature_set.name if not writer.write_to_entity else feature_set.entity
        )

        fs_schema = writer.db_config.translate(feature_set.get_schema())
        db_schema = self._get_schema(table_name, writer.database)

        queries = self.create_query(
            fs_schema, table_name, db_schema, writer.write_to_entity
        )

        if debug_mode:
            print(
                "#### DEBUG MODE ###\n"
                f"Feature set: {feature_set.name}\n"
                "Queries:\n"
                f"{queries}"
            )
        else:
            for q in queries:
                logger.info(f"Applying this query: {q} ...")
                self._client.sql(q)

            logger.info(f"Feature Set migration finished successfully.")
