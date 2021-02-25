"""Migration entity."""

import atexit
import io
import logging
from abc import ABC, abstractmethod
from typing import Any, Callable, Dict, List

import boto3

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
        self.logger.info("Creating query for migration!")

    def _validate_schema(
        self, fs_schema: List[Dict[str, Any]], db_schema: List[Dict[str, Any]]
    ) -> Any:
        """Provides schema validation for feature sets.

        Compares the schema of your local feature set to the
        corresponding table in a given database.

        Args:
            fs_schema: object that contains feature set's schemas.
            db_schema: object that contains the table og a given db schema.

        """

    def _get_schema(self, db_client: Callable, table_name: str) -> List[Dict[str, Any]]:
        """Get a table schema in the respective database.

        Returns:
            Schema object.
        """
        pass

    def _apply_migration(self, query: str, db_client: Callable) -> None:
        """Apply the migration in the respective database."""

    @staticmethod
    def _write_logs(body, bucket, key) -> None:
        """Send all migration logs to S3."""
        s3 = boto3.client("s3")
        s3.put_object(Body=body, Bucket=bucket, Key=key)

    def _send_logs_to_s3(self) -> None:
        """Send all migration logs to S3."""
        log_stringio = self._setup_logging()
        atexit.register(
            self._write_logs,
            body=log_stringio.getvalue(),
            bucket="bucket_name",
            key="key_name",
        )
        pass

    @staticmethod
    def _setup_logging() -> io.StringIO:
        logger = logging.getLogger()
        log_stringio = io.StringIO()
        handler = logging.StreamHandler(log_stringio)
        logger.addHandler(handler)

        return log_stringio

    def run(self, pipelines: List[FeatureSetPipeline]) -> None:
        """Runs the migrations.

        Args:
            pipelines: the feature set pipelines.

        """
        pass
