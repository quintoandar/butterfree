"""Migration entity."""
import logging
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

        diff_list = list(filterfalse(lambda x: x in db_schema, fs_schema))

        for feature in diff_list:
            matching_features = [
                x for x in db_schema if x["column_name"] == feature["column_name"]
            ]

            if matching_features:
                raise ValueError(f"The {feature['column_name']} can't be changed.")

            mismatches.append(feature)

        return None if mismatches == [] else mismatches

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
