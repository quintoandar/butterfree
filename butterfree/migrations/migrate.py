"""Holds the Migrator Class."""
from functools import reduce
from typing import List, Tuple

from butterfree.constants.databases import ALLOWED_DATABASE
from butterfree.load.writers.writer import Writer
from butterfree.pipelines import FeatureSetPipeline
from butterfree.transform import FeatureSet


class Migrate:
    """Execute migration operations in a Database based on pipeline Writer.

    Attributes:
        pipelines: list of Feature Set Pipelines to use to migration.

    """

    def __init__(self, pipelines: List[FeatureSetPipeline]) -> None:
        self.pipelines = pipelines

    @staticmethod
    def _parse_feature_set_pipeline(
        pipeline: FeatureSetPipeline,
    ) -> Tuple[Writer, FeatureSet]:

        feature_set = pipeline.feature_set

        return reduce(
            lambda feature_set, db_migration: (db_migration, feature_set),
            pipeline.sink.writers,
            feature_set,
        )

    def _send_logs_to_s3(self) -> None:
        """Send all migration logs to S3."""
        pass

    def migration(self) -> None:
        """Construct and apply the migrations."""
        migration_list = [
            self._parse_feature_set_pipeline(pipeline) for pipeline in self.pipelines
        ]

        for writer, fs in migration_list:
            migration = ALLOWED_DATABASE[writer.db_config._database_migration]
            migration.run(fs)
