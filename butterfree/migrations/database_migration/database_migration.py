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

    def _get_diff(
        self, fs_schema: List[Dict[str, Any]], db_schema: List[Dict[str, Any]] = None,
    ) -> List[Dict[str, Any]]:
        """Gets schema difference between feature set and the table of a given db.

        Args:
            fs_schema: object that contains feature set's schemas.
            db_schema: object that contains the table of a given db schema.

        """
        if not db_schema:
            return fs_schema

        schema_diff = list(filterfalse(lambda x: x in db_schema, fs_schema))

        return self._get_type_inconsistent_features(schema_diff, db_schema)

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

        Args:
            db_client: Database client.
            table_name: Table name to get schema.

        Returns:
            Schema object.
        """
        try:
            db_schema = db_client.get_schema(table_name)
        except Exception:
            db_schema = None

        return db_schema

    def apply_migration(self, feature_set: FeatureSet) -> None:
        """Apply the migration in the respective database.

        Args:
            feature_set: the feature set.

        """
        logging.info(f"Migrating feature set: {feature_set.name}")

        fs_schema = self._db_config.translate(feature_set.get_schema())

        db_schema = self._get_schema(
            db_client=self._client, table_name=feature_set.name
        )

        diff_schema = self._get_diff(fs_schema, db_schema)
        query = self.create_query(feature_set.name, db_schema, diff_schema)

        try:
            self._client.sql(query)
            logging.info(f"Success! The query applied is {query}")

        except Exception:
            logging.error(f"Migration failed!")
