"""Holds the Migrate Class."""
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
    ) -> List[Tuple[Writer, FeatureSet]]:

        feature_set = pipeline.feature_set

        return list(
            map(lambda db_migration: (db_migration, feature_set), pipeline.sink.writers)
        )

    def _send_logs_to_s3(self) -> None:
        """Send all migration logs to S3."""
        pass

    def migration(self) -> None:
        """Construct and apply the migrations."""
        for pipeline in self.pipelines:
            migration_list = self._parse_feature_set_pipeline(pipeline)

            for writer, fs in migration_list:
                migration = ALLOWED_DATABASE[writer.db_config._database_migration]
                migration.apply_migration(fs)
