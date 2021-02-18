"""Migration entity."""

from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional

from butterfree.transform import FeatureSet


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

    def _validate_schema(
        self, fs_schema: List[Dict[str, Any]], db_schema: List[Dict[str, Any]] = None,
    ) -> Any:
        """Provides schema validation for feature sets.

        Compares the schema of your local feature set to the
        corresponding table in a given database.

        Args:
            fs_schema: object that contains feature set's schemas.
            db_schema: object that contains the table og a given db schema.

        """
        mismatches = []

        if not db_schema:
            return fs_schema

        for feature in fs_schema:
            matching_features = [
                x for x in db_schema if x["column_name"] == feature["column_name"]
            ]

            if not matching_features:
                mismatches.append(feature)
                continue

            if feature["type"] == matching_features[0]["type"]:
                continue

            mismatches.append(
                {
                    "column_name": feature["column_name"],
                    "type": feature["type"],
                    "primary_key": feature["primary_key"],
                    "old_type": matching_features[0]["type"],
                }
            )

        return None if mismatches == [] else mismatches

    def _get_schema(
        self, db_client: Any, table_name: str
    ) -> Optional[List[Dict[str, Any]]]:
        """Get a table schema in the respective database.

        Returns:
            Schema object.
        """
        try:
            db_schema = db_client.get_schema(table_name)
        except RuntimeError:
            db_schema = None

        return db_schema

    def run(self, feature_set: FeatureSet) -> None:
        """Runs the migrations.

        Args:
            feature_set: the feature set.

        """
        self._apply_migration(feature_set)
