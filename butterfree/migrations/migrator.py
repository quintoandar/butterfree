from typing import List, Any

from butterfree.pipelines import FeatureSetPipeline


class Migrator:
    """Execute migration operations in a Database based on pipeline Writer.


    Attributes:
        pipelines: list of Feature Set Pipelines to use to migration.

    """
    def __init__(self, pipelines: List[FeatureSetPipeline]) -> None:
        self.pipelines = pipelines

    def _parse_feature_set_pipelines(self, pipeline: FeatureSetPipeline) -> Any:
        pass

    def migrate(self) -> None:
        pass
