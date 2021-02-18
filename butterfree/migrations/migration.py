"""Migration entity."""

from abc import ABC, abstractmethod
from typing import Any, Callable, Dict, List

from butterfree.pipelines import FeatureSetPipeline


class DatabaseMigration(ABC):
    """Abstract base class for Migrations."""

    @abstractmethod
    def create_query(
        self,
        fs_schema: List[Dict[str, Any]],
        db_schema: List[Dict[str, Any]],
        table_name: str,
    ) -> Any:
        """Create a query regarding a data source.

        Returns:
            The desired query for the given database.

        """

    def validate_schema(
        self, fs_schema: List[Dict[str, Any]], db_schema: List[Dict[str, Any]]
    ) -> Any:
        """Provides schema validation for feature sets.

        Compares the schema of your local feature set to the
        corresponding table in a given database.

        Args:
            fs_schema: object that contains feature set's schemas.
            db_schema: object that contains the table og a given db schema.

        """

    def get_schema(self, db_client: Callable, table_name: str) -> List[Dict[str, Any]]:
        """Get a table schema in the respective database.

        Returns:
            Schema object.
        """
        pass

    def apply_migration(self, query: str, db_client: Callable) -> None:
        """Apply the migration in the respective database."""

    def send_logs_to_s3(self) -> None:
        """Send all migration logs to S3."""
        pass

    def run(self, pipelines: List[FeatureSetPipeline]) -> None:
        """Runs the migrations.

        Args:
            pipelines: the feature set pipelines.

        """
        pass
