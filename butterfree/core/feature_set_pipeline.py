"""FeatureSetPipeline entity."""

from dataclasses import dataclass
from typing import List
from unittest.mock import Mock

from butterfree.core.client import SparkClient
from butterfree.core.source import Source, SourceSelector
from butterfree.core.transform.feature import Feature


@dataclass
class FeatureSetPipeline:
    """Defines a FeatureSetPipeline.

    Attributes:
        name: feature set name.
        entity: feature set entity.
        description: brief explanation regarding the feature set.
        readers: list of readers.
        query: query defined by user.
        features: list of desired features.
        writers: list of writers.
    """

    name: str
    entity: str
    description: str
    readers: List[Source]
    query: str
    features: List[Feature]
    writers: List[str]

    def run(self):
        """Runs feature set pipeline."""
        spark_client = SparkClient()
        source_selector = SourceSelector(
            spark_client=spark_client, sources=self.readers, query=self.query
        )
        dataframe = source_selector.construct()
        feature_set = Mock(
            name=self.name,
            entity=self.entity,
            description=self.description,
            features=[self.features],
        )
        dataframe = feature_set.construct(dataframe)
        sink = Mock(feature_set=feature_set, writers=[self.writers])
        sink.flush(dataframe)
