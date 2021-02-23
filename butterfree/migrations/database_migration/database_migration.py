"""Migration entity."""
from abc import ABC, abstractmethod
from itertools import filterfalse
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

    @staticmethod
    def _get_diff(
        fs_schema: List[Dict[str, Any]], db_schema: List[Dict[str, Any]] = None,
    ) -> List[Dict[str, Any]]:
        """Gets schema difference between feature set and the table of a given db.

        Args:
            fs_schema: object that contains feature set's schemas.
            db_schema: object that contains the table of a given db schema.

        """
        if not db_schema:
            return fs_schema

        schema_diff = list(filterfalse(lambda x: x in db_schema, fs_schema))

        return schema_diff

    @staticmethod
    def _get_type_inconsistent_features(
        schema_diff: List[Dict[str, Any]], db_schema: List[Dict[str, Any]]
    ) -> List[Dict[str, Any]]:
        """Gets data types inconsistencies between features.

        Args:
            schema_diff: object that contains difference between feature
            set and the table of a given db.
            db_schema: object that contains the table of a given db schema.

        """
        features_with_diff_types = []
        for feature in schema_diff:
            matching_features = [
                x for x in db_schema if x["column_name"] == feature["column_name"]
            ]

            if matching_features:
                features_with_diff_types.append(feature)

        return features_with_diff_types

    def _get_schema(
        self, db_client: Any, table_name: str
    ) -> Optional[List[Dict[str, Any]]]:
        """Get a table schema in the respective database.

        Returns:
            Schema object.
        """
        pass

    def run(self, feature_set: FeatureSet) -> None:
        """Runs the migrations.

        Args:
            feature_set: the feature set.

        """
        pass
