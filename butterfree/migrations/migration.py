"""Migration entity."""

from abc import ABC, abstractmethod
from typing import Any, Callable


class Migration(ABC):
    """Abstract base class for Migrations."""

    @abstractmethod
    def run_query(self) -> Any:
        """Execute a query regarding a data source.

        Returns:
            Schema object.

        """

    def validate_schema(self, schema_object: Callable) -> Any:
        """Provides schema validation for feature sets.

        Compares the schema of your local feature set to the
        corresponding table in a given database.

        Args:
            schema_object: object that contains feature set's schemas.

        """

    @abstractmethod
    def apply_migration(self) -> None:
        """Apply the migration in the respective database."""

    def send_logs_to_s3(self) -> None:
        """Send all migration logs to S3."""
        pass
