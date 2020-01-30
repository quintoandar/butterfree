"""FeatureSetPipeline entity."""

from butterfree.core.reader import Source
from butterfree.core.transform import FeatureSet
from butterfree.core.writer import Sink


class FeatureSetPipeline:
    """Defines a FeatureSetPipeline.

    Attributes:
        source: sources defined by user.
        feature_set: feature set defined by user containing features.
        sink: sink used to write dataframes in the desired location.
    """

    def __init__(self, source: Source, feature_set: FeatureSet, sink: Sink):
        self.source = source
        self.feature_set = feature_set
        self.sink = sink

    def run(self):
        """Runs feature set pipeline."""
        dataframe = self.source.construct()
        dataframe = self.feature_set.construct(input_df=dataframe)
        self.sink.flush(dataframe=dataframe, feature_set=self.feature_set)
        self.sink.validate(dataframe=dataframe, feature_set=self.feature_set)
