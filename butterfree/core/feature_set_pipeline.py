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

    @property
    def source(self):
        """Attribute "source" getter.

        :return source: source entity
        """
        return self._source

    @source.setter
    def source(self, source: Source):
        """Attribute "source" setter.

        :param source: used to set attribute "source".
        """
        if not isinstance(source, Source):
            raise ValueError("source must be a Source instance")
        self._source = source

    @property
    def feature_set(self):
        """Attribute "feature_set" getter.

        :return feature_set: feature_set entity
        """
        return self._feature_set

    @feature_set.setter
    def feature_set(self, feature_set: FeatureSet):
        """Attribute "feature_set" setter.

        :param feature_set: used to set attribute "feature_set".
        """
        if not isinstance(feature_set, FeatureSet):
            raise ValueError("feature_set must be a FeatureSet instance")
        self._feature_set = feature_set

    @property
    def sink(self):
        """Attribute "sink" getter.

        :return sink: sink entity
        """
        return self._sink

    @sink.setter
    def sink(self, sink: Sink):
        """Attribute "sink" setter.

        :param sink: used to set attribute "sink".
        """
        if not isinstance(sink, Sink):
            raise ValueError("sink must be a Sink instance")
        self._sink = sink

    def run(self):
        """Runs feature set pipeline."""
        dataframe = self.source.construct()
        dataframe = self.feature_set.construct(input_df=dataframe)
        self.sink.flush(dataframe=dataframe, feature_set=self.feature_set)
        self.sink.validate(dataframe=dataframe, feature_set=self.feature_set)
