"""Holds configurations for Delta Lake operations."""

from typing import Any, Dict, List, Optional

from butterfree.configs.db import AbstractWriteConfig


class DeltaConfig(AbstractWriteConfig):
    """Configuration for Delta Lake operations.

    Attributes:
        database: Target database name for the Delta table.
        table: Target table name for the Delta table.
        merge_on: List of columns to use as merge keys.
        when_not_matched_insert: Optional condition for insert operations.
        when_matched_update: Optional condition for update operations.
        when_matched_delete: Optional condition for delete operations.
    """

    def __init__(
        self,
        database: str,
        table: str,
        merge_on: List[str],
        when_not_matched_insert: Optional[str] = None,
        when_matched_update: Optional[str] = None,
        when_matched_delete: Optional[str] = None,
    ):
        self.database = database
        self.table = table
        self.merge_on = merge_on
        self.when_not_matched_insert = when_not_matched_insert
        self.when_matched_update = when_matched_update
        self.when_matched_delete = when_matched_delete

    @property
    def database(self) -> str:
        """Database name."""
        return self.__database

    @database.setter
    def database(self, value: str) -> None:
        if not value:
            raise ValueError("Config 'database' cannot be empty.")
        self.__database = value

    @property
    def table(self) -> str:
        """Table name."""
        return self.__table

    @table.setter
    def table(self, value: str) -> None:
        if not value:
            raise ValueError("Config 'table' cannot be empty.")
        self.__table = value

    @property
    def merge_on(self) -> List[str]:
        """List of columns to use as merge keys."""
        return self.__merge_on

    @merge_on.setter
    def merge_on(self, value: List[str]) -> None:
        if not value:
            raise ValueError("Config 'merge_on' cannot be empty.")
        self.__merge_on = value

    @property
    def mode(self) -> str:
        """Write mode for Spark."""
        return "overwrite"

    @property
    def format_(self) -> str:
        """Write format for Spark."""
        return "delta"

    @property
    def when_not_matched_insert(self) -> Optional[str]:
        """Condition for insert operations."""
        return self.__when_not_matched_insert

    @when_not_matched_insert.setter
    def when_not_matched_insert(self, value: Optional[str]) -> None:
        self.__when_not_matched_insert = value

    @property
    def when_matched_update(self) -> Optional[str]:
        """Condition for update operations."""
        return self.__when_matched_update

    @when_matched_update.setter
    def when_matched_update(self, value: Optional[str]) -> None:
        self.__when_matched_update = value

    @property
    def when_matched_delete(self) -> Optional[str]:
        """Condition for delete operations."""
        return self.__when_matched_delete

    @when_matched_delete.setter
    def when_matched_delete(self, value: Optional[str]) -> None:
        self.__when_matched_delete = value

    def get_options(self, key: str) -> Dict[str, Any]:
        """Get options for Delta Lake operations.

        Args:
            key: table name in Delta Lake.

        Returns:
            Configuration for Delta Lake operations.
        """
        return {
            "table": self.table,
            "database": self.database,
        }

    def translate(self, schema: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Get feature set schema to be translated.

        Delta Lake uses the same types as Spark SQL, so no translation is needed.

        Args:
            schema: Feature set schema in Spark format.

        Returns:
            The same schema, as Delta Lake uses Spark SQL types.
        """
        return schema
