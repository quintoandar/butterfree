"""Cassandra Migration entity."""

import logging
from typing import Any

from butterfree.clients import CassandraClient
from butterfree.configs.db import CassandraConfig
from butterfree.migrations import Migration
from butterfree.pipelines import FeatureSetPipeline

FORMAT_RED = "\033[0;91m"
FORMAT_CYAN = "\033[0;96m"
FORMAT_GREEN = "\033[92m"
FORMAT_BOLD = "\033[0;1m"
FORMAT_BOLD_RED = "\033[1;91m"
FORMAT_BOLD_GREEN = "\033[1;92m"
FORMAT_BOLD_UNDERLINED_RED = "\033[1;4;91m"
CLEAR_FORMATTING = "\033[0m"


class CassandraMigration(Migration):
    """Cassandra class for Migrations."""

    @staticmethod
    def create_query(
        feature_set_schema,
        cassandra_features: list,
        cassandra_client: CassandraClient,
        feature_set_pipeline: FeatureSetPipeline,
    ):
        cassandra_features_names = []

        for feature in cassandra_features:
            cassandra_features_names.append(feature["column_name"])

        features_to_add = [
            {"column_name": x.name, "type": x.type}
            for x in feature_set_schema
            if x.name not in cassandra_features_names
        ]

        if not features_to_add:
            logging.info("No-op")
            return
        # This could be the apply migration step
        logging.info(
            f"{FORMAT_CYAN}Adding columns: {features_to_add}{CLEAR_FORMATTING}"
        )
        cassandra_client.add_columns_to_table(
            features_to_add, feature_set_pipeline.feature_set.name
        )
        logging.info(f"{FORMAT_GREEN}Success{CLEAR_FORMATTING}")

    @staticmethod
    def _create_cassandra_table(
        feature_set_schema, table_name, cassandra_client: CassandraClient
    ):
        cassandra_client.create_table(feature_set_schema, table_name)

    def apply_migration(
        self,
        feature_set_pipeline: FeatureSetPipeline,
        cassandra_config: CassandraConfig,
        client: CassandraClient,
    ) -> Any:
        """Apply the migration in Cassandra."""
        failures = 0
        feature_set_schema = cassandra_config.translate(
            schema=feature_set_pipeline.feature_set.get_schema()
        )
        try:
            cassandra_table_schema = client.get_schema(
                table=feature_set_pipeline.feature_set.name
            )
            self.validate_schema(feature_set_schema, cassandra_table_schema)
            self.create_query(cassandra_table_schema, client)

        except AssertionError as type_mismatch:
            logging.error(type_mismatch)
            failures += 1

        except RuntimeError:
            logging.info(f"{FORMAT_BOLD}Creating table{CLEAR_FORMATTING}")
            self._create_cassandra_table(
                feature_set_schema, feature_set_pipeline.feature_set.name, client
            )
            logging.info(f"{FORMAT_GREEN}Success{CLEAR_FORMATTING}")

        if failures:
            failure_message = (
                f"{FORMAT_BOLD_UNDERLINED_RED}"
                f"\nNo migrations were performed. Inconsistencies found.\n"
                f"{FORMAT_BOLD_RED}"
                "\nFix the data_type of the mismatching features in Wonka or drop the"
                " respective columns in Cassandra and rerun this migration to recreate'em\n"
                f"{CLEAR_FORMATTING}"
            )
            logging.error(failure_message)
            raise RuntimeError(failure_message)

        logging.info(
            f"{FORMAT_BOLD_GREEN}Successfully ran migrate step{CLEAR_FORMATTING}"
        )
