"""Migration entity."""

import logging
from abc import ABC, abstractmethod
from typing import Any, List, Tuple

from butterfree.pipelines import FeatureSetPipeline
from butterfree.transform import FeatureSet

FORMAT_RED = "\033[0;91m"
FORMAT_CYAN = "\033[0;96m"
FORMAT_GREEN = "\033[92m"
FORMAT_BOLD = "\033[0;1m"
FORMAT_BOLD_RED = "\033[1;91m"
FORMAT_BOLD_GREEN = "\033[1;92m"
FORMAT_BOLD_UNDERLINED_RED = "\033[1;4;91m"
CLEAR_FORMATTING = "\033[0m"


class Migration(ABC):
    """Abstract base class for Migrations."""

    @abstractmethod
    def create_query(self, fs_schema, db_schema, table_name) -> Any:
        """Create a query regarding a data source.

        Returns:
            Schema object.

        """

    @staticmethod
    def validate_schema(fs_schema, db_schema) -> Any:
        """Provides schema validation for feature sets.

        Compares the schema of your local feature set to the
        corresponding table in a given database.

        Args:
            fs_schema: object that contains feature set's schemas.
            db_schema: object that contains db table schema.

        """
        mismatches = []

        for feature in fs_schema:
            matching_features = [
                x for x in db_schema if x["column_name"] == feature.name
            ]

            if not matching_features:
                continue

            if feature.type == matching_features[0]["type"]:
                continue

            mismatches.append(
                (feature.name, feature.type, matching_features[0]["type"])
            )

    @abstractmethod
    def apply_migration(self, query, db_client) -> None:
        """Apply the migration in the respective database."""

    @staticmethod
    def _parse_feature_set_pipeline(
        feature_set_pipeline: FeatureSetPipeline,
    ) -> List[Tuple[str, FeatureSet]]:
        feature_set = feature_set_pipeline.feature_set
        writers = [
            writer.dbconfig._migration_class
            for writer in feature_set_pipeline.sink.writers
        ]

        return [(writer, feature_set) for writer in writers]

    def send_logs_to_s3(self) -> None:
        """Send all migration logs to S3."""
        pass

    def migration(self, pipelines: List[FeatureSetPipeline]) -> None:
        """Construct and apply the migrations"""
        db_list = [self._parse_feature_set_pipeline(pipeline) for pipeline in pipelines]

        for db_client, fs in db_list:
            try:
                db_schema = db_client.get_table_schema(table_name=fs.name)
            except RuntimeError:
                continue
            fs_schema = fs.get_schema()

            self.validate_schema(fs_schema, db_schema)
            query = self.create_query(fs_schema, db_schema, fs.name)
            self.apply_migration(db_client, query)
            self.send_logs_to_s3()
