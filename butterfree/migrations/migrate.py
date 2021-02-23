"""Holds the Migrator Class."""

from typing import Callable, List, Tuple

from butterfree.pipelines import FeatureSetPipeline
from butterfree.transform import FeatureSet


class Migrate:
    """Execute migration operations in a Database based on pipeline Writer.

    Attributes:
        pipelines: list of Feature Set Pipelines to use to migration.

    """

    def __init__(self, pipelines: List[FeatureSetPipeline]) -> None:
        self.pipelines = pipelines

    def _parse_feature_set_pipeline(
        self, pipeline: FeatureSetPipeline
    ) -> List[Tuple[Callable, FeatureSet]]:
        feature_set = pipeline.feature_set
        migrations = [
            writer.db_config._migration_class for writer in pipeline.sink.writers
        ]

        return [(migrate, feature_set) for migrate in migrations]

    def _send_logs_to_s3(self) -> None:
        """Send all migration logs to S3."""
        pass

    def migration(self) -> None:
        """Construct and apply the migrations."""
        migration_list = [
            self._parse_feature_set_pipeline(pipeline) for pipeline in self.pipelines
        ]

        for migration, fs in migration_list:
            migration.run(fs)
