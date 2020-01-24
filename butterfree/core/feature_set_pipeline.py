"""FeatureSetPipeline entity."""

from typing import List
from unittest.mock import Mock

from butterfree.core.client import SparkClient
from butterfree.core.source import Source, SourceSelector
from butterfree.core.transform.feature import Feature


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

    def __init__(
        self,
        name: str,
        entity: str,
        description: str,
        readers: List[Source],
        query: str,
        features: List[Feature],
        writers: List[str],
    ):
        self.name = name
        self.entity = entity
        self.description = description
        self.readers = readers
        self.query = query
        self.features = features
        self.writers = writers
        self.feature_set = Mock
        self.sink = Mock

    def run(self):
        """Runs feature set pipeline."""
        spark_client = SparkClient()
        source_selector = SourceSelector(
            spark_client=spark_client, sources=self.readers, query=self.query
        )
        dataframe = source_selector.construct()
        feature_set = self.feature_set(
            name=self.name,
            entity=self.entity,
            description=self.description,
            features=[self.features],
        )
        dataframe = feature_set.construct(dataframe)
        sink = self.sink(feature_set=feature_set, writers=[self.writers])
        sink.construct(dataframe)
