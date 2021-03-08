"""Migration entity."""
from abc import ABC, abstractmethod
from dataclasses import dataclass
from enum import Enum, auto
from typing import Any, Dict, List, Set

from butterfree.transform import FeatureSet


@dataclass
class Diff:
    """DataClass to help identifying different types of diff between schemas."""

    class Kind(Enum):
        """Mapping actions to take given a difference between columns of a schema."""

        ADD = auto()
        DROP = auto()
        ALTER_TYPE = auto()
        ALTER_KEY = auto()

    column: str
    kind: Kind
    value: Any

    def __hash__(self):
        return hash((self.column, self.kind, self.value))

    def __eq__(self, other):
        if not isinstance(other, type(self)):
            return NotImplemented
        return (
            self.column == other.column
            and self.kind == other.kind
            and self.value == other.value
        )


class DatabaseMigration(ABC):
    """Abstract base class for Migrations."""

    @abstractmethod
    def create_query(
        self,
        table_name: str,
        db_schema: List[Dict[str, Any]] = None,
        diff_schema: List[Dict[str, Any]] = None,
    ) -> Any:
        """Create a query regarding a data source.

        Returns:
            The desired query for the given database.

        """

    def _apply_migration(self, feature_set: FeatureSet) -> None:
        """Apply the migration in the respective database."""
        pass

    @staticmethod
    def _get_diff(
        fs_schema: List[Dict[str, Any]], db_schema: List[Dict[str, Any]],
    ) -> Set[Diff]:
        """Gets schema difference between feature set and the table of a given db.

        Args:
            fs_schema: object that contains feature set's schemas.
            db_schema: object that contains the table of a given db schema.

        """
        db_columns = set(item.get("column_name") for item in db_schema)
        fs_columns = set(item.get("column_name") for item in fs_schema)

        add_columns = fs_columns - db_columns
        drop_columns = db_columns - fs_columns

        # This could be way easier to write (and to read) if the schemas were a simple
        # Dict[str, Any] where each key would be the column name itself...
        # but changing that could break things so:
        # TODO version 2 change get schema to return a dict(columns, properties)
        alter_type_columns = dict()
        alter_key_columns = dict()
        for fs_item in fs_schema:
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
            Diff(col, kind=Diff.Kind.ADD, value=None) for col in add_columns
        )
        schema_diff |= set(
            Diff(col, kind=Diff.Kind.DROP, value=None) for col in drop_columns
        )
        schema_diff |= set(
            Diff(col, kind=Diff.Kind.ALTER_TYPE, value=value)
            for col, value in alter_type_columns.items()
        )
        schema_diff |= set(
            Diff(col, kind=Diff.Kind.ALTER_KEY, value=value)
            for col, value in alter_key_columns.items()
        )
        return schema_diff

    def run(self, feature_set: FeatureSet) -> None:
        """Runs the migrations.

        Args:
            feature_set: the feature set.

        """
        pass
