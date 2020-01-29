"""FeatureSetPipeline entity."""

from butterfree.core.reader import Source
from butterfree.core.transform import FeatureSet


class FeatureSetPipeline:
    """Defines a FeatureSetPipeline.

    Attributes:
        source: sources.
        feature_set: feature sets.
        sink: sinks.
    """

    def __init__(self, source: Source, feature_set: FeatureSet, sink):
        self.source = source
        self.feature_set = feature_set
        self.sink = sink

    def run(self):
        """Runs feature set pipeline."""
        dataframe = self.source.construct()
        dataframe = self.feature_set.construct(dataframe)
        self.sink.flush(dataframe, self.feature_set)
        self.sink.validate(dataframe)
