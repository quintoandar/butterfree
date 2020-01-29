"""FeatureSetPipeline entity."""

from dataclasses import dataclass
from typing import List
from unittest.mock import Mock

from butterfree.core.client import SparkClient
from butterfree.core.reader import Reader, Source
from butterfree.core.transform import Feature, FeatureSet
from butterfree.core.writer import Writer


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
    key_columns: List[str]
    readers: List[Reader]
    query: str
    features: List[Feature]
    timestamp_column: str
    writers: List[Writer]

    def run(self):
        """Runs feature set pipeline."""
        spark_client = SparkClient()
        source = Source(
            spark_client=spark_client, readers=self.readers, query=self.query
        )
        dataframe = source.construct()
        feature_set = FeatureSet(
            name=self.name,
            entity=self.entity,
            description=self.description,
            features=self.features,
            key_columns=self.key_columns,
            timestamp_column=self.timestamp_column,
        )
        dataframe = feature_set.construct(dataframe)
        sink = Mock(feature_set=feature_set, writers=self.writers)
        sink.flush(dataframe)
