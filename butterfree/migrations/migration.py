"""Migration entity."""

import logging
from abc import ABC, abstractmethod
from typing import Any, List, Tuple, Dict

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
    def create_query(self, feature_set_pipeline, config, client) -> Any:
        """Create a query regarding a data source.

        Returns:
            Schema object.

        """

    @staticmethod
    def validate_schema(local_schema_object: List[Dict[str, Any]], db_schema_object: List[Dict[str, Any]]) -> Any:
        """Provides schema validation for feature sets.

        Compares the schema of your local feature set to the
        corresponding table in a given database.

        Args:
            local_schema_object: object that contains feature set's schemas.
            db_schema_object: object that contains db table schema.

        """
        mismatches = []

        for feature in local_schema_object:
            matching_features = [
                x for x in db_schema_object if x["column_name"] == feature["column_name"]
            ]

            if not matching_features:
                continue

            if feature["type"] == matching_features[0]["type"]:
                continue

            mismatches.append(
                (feature["column_name"], feature["type"], matching_features[0]["type"])
            )

        error_message = (
            f"{FORMAT_RED}"
            f"\nFeatures types mismatches found between Feature Set and Database:"
            f"{CLEAR_FORMATTING}"
        )

        for feature, fs_type, db_type in mismatches:
            error_message += (
                f"{FORMAT_RED}"
                f"\nColumn '{feature}' type is inconsistent:"
                f" '{fs_type}' (Feature Set) != '{db_type}' (Database)"
                f"{CLEAR_FORMATTING}"
            )

        assert not mismatches, error_message

        logging.info(
            f"{FORMAT_BOLD_GREEN}" f"Entity is consistent \\o/" f"{CLEAR_FORMATTING}"
        )

    @abstractmethod
    def get_schema(self, feature_set: FeatureSet) -> List[Dict[str, Any]]:
        """Get a table schema in the respective database.

        Returns:
            Schema object.

        """

    @abstractmethod
    def apply_migration(self, *args, **kwargs) -> None:
        """Apply the migration in the respective database."""

    def _parse_feature_set_pipeline(self, feature_set_pipeline: FeatureSetPipeline) -> List[Tuple[str, FeatureSet]]:
        feature_set = feature_set_pipeline.feature_set
        writers = [writer.db_config._migration_class for writer in feature_set_pipeline.sink.writers]

        return [(writer, feature_set) for writer in writers]

    def migration(self, pipelines: List[FeatureSetPipeline]) -> None:
        """Construct and apply the migrations"""
        db_list = [self._parse_feature_set_pipeline(pipeline) for pipeline in pipelines]

        for db, fs in db_list:
            db_schema = db.get_table_schema(feature_set=fs)
            fs_schema = fs.get_schema()

            self.validate_schema(fs_schema, db_schema)
            self.create_query()
            self.apply_migration()

    def send_logs_to_s3(self) -> None:
        """Send all migration logs to S3."""
        pass
