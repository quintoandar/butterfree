"""Migration entity."""

from abc import ABC, abstractmethod
from typing import Any, Callable


class Migration(ABC):
    """Abstract base class for Migrations."""

    @abstractmethod
    def run_query(self, client: Callable) -> None:
        """Execute a query regarding a data source.

        Returns:
            Schema object.

        """

    def validate_schemas(self, schema_object: Callable) -> Any:
        """Provides schema validation for feature sets.

        Compares the schema of your local feature set to the
        corresponding table in a given database.

        All the transformations are used when the method consume is called.

        Args:
            schema_object: object that contains feature set's schemas.

        Returns:
            Reader object with new transformation

        """

    @abstractmethod
    def apply_migration(self) -> None:
        """Extract data from target origin.

        Args:
            client: client responsible for connecting to Spark session.

        Returns:
            Dataframe with all the data.

        :return: Spark dataframe
        """

    def send_logs_to_s3(self) -> None:
        pass
